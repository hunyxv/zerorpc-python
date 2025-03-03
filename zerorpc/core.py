# -*- coding: utf-8 -*-
# Open Source Initiative OSI - The MIT License (MIT):Licensing
#
# The MIT License (MIT)
# Copyright (c) 2015 François-Xavier Bourlet (bombela+zerorpc@gmail.com)
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is furnished to do
# so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


from __future__ import absolute_import
from builtins import str
from builtins import zip
from future.utils import iteritems

import sys
import traceback
import gevent.pool
import gevent.queue
import gevent.event
import gevent.local
import gevent.lock

from . import gevent_zmq as zmq
from .exceptions import TimeoutExpired, RemoteError, LostRemote
from .channel import ChannelMultiplexer, BufferedChannel
from .socket import SocketBase
from .heartbeat import HeartBeatOnChannel
from .context import Context
from .decorators import DecoratorBase, rep
from . import patterns
from logging import getLogger

logger = getLogger(__name__)


class ServerBase(object):

    def __init__(self, channel, methods=None, name=None, context=None,
            pool_size=None, heartbeat=5):
        self._multiplexer = ChannelMultiplexer(channel)

        if methods is None:
            methods = self

        self._context = context or Context.get_instance()
        self._name = name or self._extract_name()
        self._task_pool = gevent.pool.Pool(size=pool_size)
        self._acceptor_task = None
        self._methods = self._filter_methods(ServerBase, self, methods)

        self._inject_builtins()
        self._heartbeat_freq = heartbeat

        for (k, functor) in iteritems(self._methods):       # 着这里对 用户写的task函数装饰了一次
            if not isinstance(functor, DecoratorBase):
                self._methods[k] = rep(functor)

    @staticmethod
    def _filter_methods(cls, self, methods):
        if isinstance(methods, dict):
            return methods
        server_methods = set(k for k in dir(cls) if not k.startswith('_'))  # {'debug', 'disconnect', 'close', 'run', 'bind', 'stop', 'connect'}
        return dict((k, getattr(methods, k))
                    for k in dir(methods)
                    if callable(getattr(methods, k)) and
                    not k.startswith('_') and k not in server_methods
                    )

    @staticmethod
    def _extract_name(methods):
        return getattr(methods, '__name__', None) \
            or getattr(type(methods), '__name__', None) \
            or repr(methods)

    def close(self):
        self.stop()
        self._multiplexer.close()

    def _format_args_spec(self, args_spec, r=None):      # args_spec: inspect.getargspec(func(a, b=10))--> ArgSpec(args=['a', 'b'], varargs=None, keywords=None, defaults=(10,))
        if args_spec:
            r = [dict(name=name) for name in args_spec[0]]
            default_values = args_spec[3]
            if default_values is not None:
                for arg, def_val in zip(reversed(r), reversed(default_values)):
                    arg['default'] = def_val
        return r

    def _zerorpc_inspect(self):
        methods = dict((m, f) for m, f in iteritems(self._methods)
                    if not m.startswith('_'))
        detailled_methods = dict(
            (
                m,
                dict(
                    args=self._format_args_spec(f._zerorpc_args()),  # inspect.getargspec(func(a, b=10))--> ArgSpec(args=['a', 'b'], varargs=None, keywords=None, defaults=(10,))
                    doc=f._zerorpc_doc()
                )
            ) for (m, f) in iteritems(methods)
        )             # 一个函数详细信息的字典， 函数名，函数参数，函数说明文档
        return {'name': self._name,       # methods name
                'methods': detailled_methods}

    def _inject_builtins(self):
        self._methods['_zerorpc_list'] = lambda: [m for m in self._methods
                if not m.startswith('_')]                                     # methods list
        self._methods['_zerorpc_name'] = lambda: self._name                   # methods class name
        self._methods['_zerorpc_ping'] = lambda: ['pong', self._name]         # 
        self._methods['_zerorpc_help'] = lambda m: \
            self._methods[m]._zerorpc_doc()
        self._methods['_zerorpc_args'] = \
            lambda m: self._methods[m]._zerorpc_args()
        self._methods['_zerorpc_inspect'] = self._zerorpc_inspect

    def __call__(self, method, *args):
        if method not in self._methods:
            raise NameError(method)
        return self._methods[method](*args)

    def _print_traceback(self, protocol_v1, exc_infos):  # 输出异常？
        logger.exception('')

        exc_type, exc_value, exc_traceback = exc_infos
        if protocol_v1:
            return (repr(exc_value),)
        human_traceback = traceback.format_exc()    # 把异常栈以字符串的形式返回
        name = exc_type.__name__
        human_msg = str(exc_value)
        return (name, human_msg, human_traceback)

    def _async_task(self, initial_event):
        protocol_v1 = initial_event.header.get(u'v', 1) < 2
        channel = self._multiplexer.channel(initial_event)   # 拿到第一个 event，然后创建 channel
        hbchan = HeartBeatOnChannel(channel, freq=self._heartbeat_freq,
                passive=protocol_v1)       # 心跳 channel， 其中将不是心跳的帧放到 其 queue中了 通过recv 获取
        bufchan = BufferedChannel(hbchan)      # BufferedChannel 没全看明白 里面维护两个长度(远端队列长度和本地队列长度)， 一个队列（存放将要发送的event）
    
        exc_infos = None
        event = bufchan.recv()
        try:
            self._context.hook_load_task_context(event.header)         # load_task_context hook 
            functor = self._methods.get(event.name, None)              # 根据 event.name 获取 event 相对应的函数，functor是被一个类装饰过的了
            if functor is None:
                raise NameError(event.name)
            functor.pattern.process_call(self._context, bufchan, event, functor)  # 执行函数 和 函数钩子
        except LostRemote:    # 连接中断
            exc_infos = list(sys.exc_info())
            self._print_traceback(protocol_v1, exc_infos)
        except Exception:        # 发生了其他错误 
            exc_infos = list(sys.exc_info())
            human_exc_infos = self._print_traceback(protocol_v1, exc_infos)
            reply_event = bufchan.new_event(u'ERR', human_exc_infos,    # 创建异常 event
                    self._context.hook_get_task_context())
            self._context.hook_server_inspect_exception(event, reply_event, exc_infos)
            bufchan.emit_event(reply_event)
        finally:
            del exc_infos
            bufchan.close()   # 这里也关闭了 hbchan

    def _acceptor(self):
        """
           这就是一个请求到来时最开始的地方！！！！
        """
        while True:
            initial_event = self._multiplexer.recv()                      # 拿到一个最初的 event
            self._task_pool.spawn(self._async_task, initial_event)        # 加入到协程池（执行这个函数）

    def run(self):
        self._acceptor_task = gevent.spawn(self._acceptor)
        try:
            self._acceptor_task.get()   # 执行 gevent.spawn(self._acceptor) 这个协程
        finally:
            self.stop()
            self._task_pool.join(raise_error=True)   # 等待已经正在执行的task结束

    def stop(self):
        if self._acceptor_task is not None:
            self._acceptor_task.kill()
            self._acceptor_task = None


class ClientBase(object):

    def __init__(self, channel, context=None, timeout=30, heartbeat=5,
            passive_heartbeat=False):
        self._multiplexer = ChannelMultiplexer(channel,
                ignore_broadcast=True)
        self._context = context or Context.get_instance()
        self._timeout = timeout
        self._heartbeat_freq = heartbeat
        self._passive_heartbeat = passive_heartbeat

    def close(self):
        self._multiplexer.close()

    def _handle_remote_error(self, event):
        exception = self._context.hook_client_handle_remote_error(event)
        if not exception:
            if event.header.get(u'v', 1) >= 2:
                (name, msg, traceback) = event.args
                exception = RemoteError(name, msg, traceback)
            else:
                (msg,) = event.args
                exception = RemoteError('RemoteError', msg, None)

        return exception

    def _select_pattern(self, event):
        for pattern in self._context.hook_client_patterns_list(
                patterns.patterns_list):
            if pattern.accept_answer(event):  # 判断 event 是 REP 还是 STEAM
                return pattern                # 返回 符合 的 pattern
        return None

    def _process_response(self, request_event, bufchan, timeout):
        def raise_error(ex):
            bufchan.close()
            self._context.hook_client_after_request(request_event, None, ex)  # 请求完成之后执行的钩子hook_client_after_request （此时接受发生超时 或 pattern不符合要求）
            raise ex

        try:
            reply_event = bufchan.recv(timeout=timeout)
        except TimeoutExpired:
            raise_error(TimeoutExpired(timeout,
                    'calling remote method {0}'.format(request_event.name)))

        pattern = self._select_pattern(reply_event)     # 根据 event.name 来判断响应是何类型，选择处理方式
        if pattern is None:
            raise_error(RuntimeError(
                'Unable to find a pattern for: {0}'.format(request_event)))

        return pattern.process_answer(self._context, bufchan, request_event,
                reply_event, self._handle_remote_error) # 如果是REP 则返回执行结果， 若是 STREAM 则返回一个迭代器

    def __call__(self, method, *args, **kargs):
        # here `method` is either a string of bytes or an unicode string in
        # Python2 and Python3. Python2: str aka a byte string containing ASCII
        # (unless the user explicitly provide an unicode string). Python3: str
        # aka an unicode string (unless the user explicitly provide a byte
        # string).
        # zerorpc protocol requires an utf-8 encoded string at the msgpack
        # level. msgpack will encode any unicode string object to UTF-8 and tag
        # it `string`, while a bytes string will be tagged `bin`.
        #
        # So when we get a bytes string, we assume it to be an UTF-8 string
        # (ASCII is contained in UTF-8) that we decode to an unicode string.
        # Right after, msgpack-python will re-encode it as UTF-8. Yes this is
        # terribly inefficient with Python2 because most of the time `method`
        # wll already be an UTF-8 encoded bytes string.
        if isinstance(method, bytes):
            method = method.decode('utf-8')

        timeout = kargs.get('timeout', self._timeout)
        channel = self._multiplexer.channel()
        hbchan = HeartBeatOnChannel(channel, freq=self._heartbeat_freq,
                passive=self._passive_heartbeat)
        bufchan = BufferedChannel(hbchan, inqueue_size=kargs.get('slots', 100))

        xheader = self._context.hook_get_task_context()
        request_event = bufchan.new_event(method, args, xheader)
        self._context.hook_client_before_request(request_event)  # 钩子 client_before_request
        bufchan.emit_event(request_event)

        if kargs.get('async', False) is False:   # 如果不是异步的话 就阻塞等待结果
            return self._process_response(request_event, bufchan, timeout)

        async_result = gevent.event.AsyncResult()  # .AsyncResult - 等待单一结果而阻塞,也允许引发异常.
        gevent.spawn(self._process_response, request_event, bufchan,
                timeout).link(async_result)
        return async_result

    def __getattr__(self, method):
        return lambda *args, **kargs: self(method, *args, **kargs)  # 在这执行 function 请求 ！！！


class Server(SocketBase, ServerBase):

    def __init__(self, methods=None, name=None, context=None, pool_size=None,
            heartbeat=5):
        SocketBase.__init__(self, zmq.ROUTER, context)   # zmq.ROUTER zmq 中的一种套接字 https://github.com/anjuke/zguide-cn/blob/master/chapter2.md
        if methods is None:
            methods = self

        name = name or ServerBase._extract_name(methods)
        methods = ServerBase._filter_methods(Server, self, methods)
        ServerBase.__init__(self, self._events, methods, name, context,
                pool_size, heartbeat)

    def close(self):
        ServerBase.close(self)
        SocketBase.close(self)


class Client(SocketBase, ClientBase):

    def __init__(self, connect_to=None, context=None, timeout=30, heartbeat=5,
            passive_heartbeat=False):
        SocketBase.__init__(self, zmq.DEALER, context=context)
        ClientBase.__init__(self, self._events, context, timeout, heartbeat,
                passive_heartbeat)
        if connect_to:
            self.connect(connect_to)

    def close(self):
        ClientBase.close(self)
        SocketBase.close(self)


class Pusher(SocketBase):

    def __init__(self, context=None, zmq_socket=zmq.PUSH):
        super(Pusher, self).__init__(zmq_socket, context=context)

    def __call__(self, method, *args):
        self._events.emit(method, args,
                self._context.hook_get_task_context())

    def __getattr__(self, method):
        return lambda *args: self(method, *args)


class Puller(SocketBase):

    def __init__(self, methods=None, context=None, zmq_socket=zmq.PULL):
        super(Puller, self).__init__(zmq_socket, context=context)

        if methods is None:
            methods = self

        self._methods = ServerBase._filter_methods(Puller, self, methods)
        self._receiver_task = None

    def close(self):
        self.stop()
        super(Puller, self).close()

    def __call__(self, method, *args):
        if method not in self._methods:
            raise NameError(method)
        return self._methods[method](*args)

    def _receiver(self):
        while True:
            event = self._events.recv()
            try:
                if event.name not in self._methods:
                    raise NameError(event.name)
                self._context.hook_load_task_context(event.header)
                self._context.hook_server_before_exec(event)
                self._methods[event.name](*event.args)
                # In Push/Pull their is no reply to send, hence None for the
                # reply_event argument
                self._context.hook_server_after_exec(event, None)
            except Exception:
                exc_infos = sys.exc_info()
                try:
                    logger.exception('')
                    self._context.hook_server_inspect_exception(event, None, exc_infos)
                finally:
                    del exc_infos

    def run(self):
        self._receiver_task = gevent.spawn(self._receiver)
        try:
            self._receiver_task.get()
        finally:
            self._receiver_task = None

    def stop(self):
        if self._receiver_task is not None:
            self._receiver_task.kill(block=False)


class Publisher(Pusher):

    def __init__(self, context=None):
        super(Publisher, self).__init__(context=context, zmq_socket=zmq.PUB)


class Subscriber(Puller):

    def __init__(self, methods=None, context=None):
        super(Subscriber, self).__init__(methods=methods, context=context,
                zmq_socket=zmq.SUB)
        self._events.setsockopt(zmq.SUBSCRIBE, b'')      # 服务端订阅所有


def fork_task_context(functor, context=None):
    '''Wrap a functor to transfer context.

        Usage example:
            gevent.spawn(zerorpc.fork_task_context(myfunction), args...)

        The goal is to permit context "inheritance" from a task to another.
        Consider the following example:

            zerorpc.Server receive a new event
              - task1 is created to handle this event this task will be linked
                to the initial event context. zerorpc.Server does that for you.
              - task1 make use of some zerorpc.Client instances, the initial
                event context is transfered on every call.

              - task1 spawn a new task2.
              - task2 make use of some zerorpc.Client instances, it's a fresh
                context. Thus there is no link to the initial context that
                spawned task1.

              - task1 spawn a new fork_task_context(task3).
              - task3 make use of some zerorpc.Client instances, the initial
                event context is transfered on every call.

        A real use case is a distributed tracer. Each time a new event is
        created, a trace_id is injected in it or copied from the current task
        context. This permit passing the trace_id from a zerorpc.Server to
        another via zerorpc.Client.

        The simple rule to know if a task need to be wrapped is:
            - if the new task will make any zerorpc call, it should be wrapped.
    '''
    context = context or Context.get_instance()
    xheader = context.hook_get_task_context()

    def wrapped(*args, **kargs):
        context.hook_load_task_context(xheader)
        return functor(*args, **kargs)
    return wrapped
