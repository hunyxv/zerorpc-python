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

import gevent.pool
import gevent.queue
import gevent.event
import gevent.local
import gevent.lock
import logging

from .exceptions import TimeoutExpired
from .channel_base import ChannelBase


logger = logging.getLogger(__name__)


class ChannelMultiplexer(ChannelBase):
    def __init__(self, events, ignore_broadcast=False):
        self._events = events
        self._active_channels = {}   #  存放的是一对一对的 id：通道 
        self._channel_dispatcher_task = None
        self._broadcast_queue = None
        if events.recv_is_supported and not ignore_broadcast:
            self._broadcast_queue = gevent.queue.Queue(maxsize=1)
            self._channel_dispatcher_task = gevent.spawn(
                self._channel_dispatcher)

    @property
    def recv_is_supported(self):
        return self._events.recv_is_supported

    @property
    def emit_is_supported(self):
        return self._events.emit_is_supported

    def close(self):
        if self._channel_dispatcher_task:
            self._channel_dispatcher_task.kill()

    def new_event(self, name, args, xheader=None):
        return self._events.new_event(name, args, xheader)

    def emit_event(self, event, timeout=None):
        return self._events.emit_event(event, timeout)

    def recv(self, timeout=None):
        if self._broadcast_queue is not None:
            event = self._broadcast_queue.get(timeout=timeout)
        else:
            event = self._events.recv(timeout=timeout)
        return event

    def _channel_dispatcher(self):   # channel 调度器, 给 给定的 channel 分发 event
        while True:
            try:
                event = self._events.recv()
            except Exception:
                logger.exception('zerorpc.ChannelMultiplexer ignoring error on recv')
                continue
            channel_id = event.header.get(u'response_to', None)    # 获取目标 ～～identity～～ zmq.ROUTER?  channel_id 是 message_id n122行

            queue = None
            if channel_id is not None:
                channel = self._active_channels.get(channel_id, None)   # 根据 ～～identity～～ 获取 channel, channel_id 是 message_id n122行
                if channel is not None:
                    queue = channel._queue
            elif self._broadcast_queue is not None:
                queue = self._broadcast_queue

            if queue is None:
                logger.warning('zerorpc.ChannelMultiplexer,'
                        ' unable to route event: {0}'.format(
                            event.__str__(ignore_args=True)))
            else:
                queue.put(event)

    def channel(self, from_event=None):                    # 创建一个 channel
        if self._channel_dispatcher_task is None:
            self._channel_dispatcher_task = gevent.spawn(
                self._channel_dispatcher)
        return Channel(self, from_event)

    @property
    def active_channels(self):
        return self._active_channels

    @property
    def context(self):
        return self._events.context


class Channel(ChannelBase):

    def __init__(self, multiplexer, from_event=None):
        self._multiplexer = multiplexer
        self._channel_id = None
        self._zmqid = None
        self._queue = gevent.queue.Queue(maxsize=1)
        if from_event is not None:
            self._channel_id = from_event.header[u'message_id']      # message id 就是 channel id
            self._zmqid = from_event.identity                        # 类似： b'\x00k\x8bEg'
            self._multiplexer._active_channels[self._channel_id] = self    # 把这个 channel 添加到活动的 channel 中
            logger.debug('<-- new channel %s', self._channel_id)
            self._queue.put(from_event)

    @property
    def recv_is_supported(self):
        return self._multiplexer.recv_is_supported

    @property
    def emit_is_supported(self):
        return self._multiplexer.emit_is_supported

    def close(self):
        if self._channel_id is not None:
            del self._multiplexer._active_channels[self._channel_id]
            logger.debug('-x- closed channel %s', self._channel_id)
            self._channel_id = None

    def new_event(self, name, args, xheader=None):
        event = self._multiplexer.new_event(name, args, xheader)
        if self._channel_id is None:
            self._channel_id = event.header[u'message_id']
            self._multiplexer._active_channels[self._channel_id] = self
            logger.debug('--> new channel %s', self._channel_id)
        else:
            event.header[u'response_to'] = self._channel_id   # ？这里 的 event 应该是 functer 返回的结果所创建的
        event.identity = self._zmqid
        return event

    def emit_event(self, event, timeout=None):
        self._multiplexer.emit_event(event, timeout)

    def recv(self, timeout=None):
        try:
            event = self._queue.get(timeout=timeout)
        except gevent.queue.Empty:
            raise TimeoutExpired(timeout)
        return event

    @property
    def context(self):
        return self._multiplexer.context


class BufferedChannel(ChannelBase):

    def __init__(self, channel, inqueue_size=100):
        self._channel = channel
        self._input_queue_size = inqueue_size
        self._remote_queue_open_slots = 1    # ？？    远端队列剩余空间大小？？
        self._input_queue_reserved = 1       # ？？  # 当前已经缓存保存的数量？
        self._remote_can_recv = gevent.event.Event()
        self._input_queue = gevent.queue.Queue()    # ？？ 缓存队列
        self._verbose = False
        self._on_close_if = None
        self._recv_task = gevent.spawn(self._recver)

    @property
    def recv_is_supported(self):
        return self._channel.recv_is_supported

    @property
    def emit_is_supported(self):
        return self._channel.emit_is_supported

    @property
    def on_close_if(self):
        return self._on_close_if

    @on_close_if.setter
    def on_close_if(self, cb):
        self._on_close_if = cb

    def close(self):
        if self._recv_task is not None:
            self._recv_task.kill()
            self._recv_task = None
        if self._channel is not None:
            self._channel.close()
            self._channel = None

    def _recver(self):
        while True:
            event = self._channel.recv()
            if event.name == u'_zpc_more':        # ？？？？？ 
                try:
                    self._remote_queue_open_slots += int(event.args[0])   # n242 行 open_slots  获取远端队列大小？？？
                except Exception:
                    logger.exception('gevent_zerorpc.BufferedChannel._recver')
                if self._remote_queue_open_slots > 0:        #  当远端队列有空位置了 设置标志 --> `远端可以接收了`
                    self._remote_can_recv.set()
            elif self._input_queue.qsize() == self._input_queue_size:   # _input_queue.qsize 表示的是当前队列中有多少数据 而不是队列的容量
                raise RuntimeError(
                    'BufferedChannel, queue overflow on event:', event)
            else:
                self._input_queue.put(event)           # ？？ 缓存
                if self._on_close_if is not None and self._on_close_if(event):  # ？？
                    self._recv_task = None
                    self.close()
                    return

    def new_event(self, name, args, xheader=None):
        return self._channel.new_event(name, args, xheader)

    def emit_event(self, event, timeout=None):
        if self._remote_queue_open_slots == 0:
            self._remote_can_recv.clear()
            self._remote_can_recv.wait(timeout=timeout)  # 远端队列没有空位置了，堵塞。。
        self._remote_queue_open_slots -= 1             # 发送给远端一个任务， 那么远端的队列大小 -1
        try:
            self._channel.emit_event(event)
        except:
            self._remote_queue_open_slots += 1
            raise

    def _request_data(self):
        open_slots = self._input_queue_size - self._input_queue_reserved  # 剩余空间大小
        self._input_queue_reserved += open_slots                 
        self._channel.emit(u'_zpc_more', (open_slots,)) # channel_base.py ChannelBase.emit(event name, args, xheader=None, timeout=None) 把 剩余空间大小 发给客户端

    def recv(self, timeout=None):
        # self._channel can be set to None by an 'on_close_if' callback if it
        # sees a suitable message from the remote end...
        #
        if self._verbose and self._channel:                # 接收第 2 帧、第 3 帧等就开始执行这个函数了
            if self._input_queue_reserved < self._input_queue_size // 2:   
                self._request_data()                       # 执行这个函数后 _input_queue_reserved = self._input_queue_size 了， 接收下一帧时 就要判断 self._input_queue_reserved < self._input_queue_size // 2
        else:                                              # 接收第 1 帧 时修改这个标识
            self._verbose = True

        try:
            event = self._input_queue.get(timeout=timeout)
        except gevent.queue.Empty:
            raise TimeoutExpired(timeout)

        self._input_queue_reserved -= 1   # 取出来一个 已存储计数 -1  ； 接受第一帧后 _input_queue_reserved = 1 - 1 = 0
        return event

    @property
    def channel(self):
        return self._channel

    @property
    def context(self):
        return self._channel.context
