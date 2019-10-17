import zerorpc

class middleware():
    def resolve_endpoint(self, endpoint):
        print(endpoint)
        # do something
        return endpoint

    def client_before_request(self, event):     # 在 client 端发出请求前调用
        print('client_before_request')

    def server_before_exec(self, event):        # 执行 task 之前调用
        print('event args', event.args)             # ['RPC']
        print('event: ', event)      # <b'\x00k\x8bEg'> hello {'message_id': b'df0428dc367a4f149c367153d631cb18', 'v': 3} ['RPC'] : identity, name, header, args
        print('server_before_exec success')
    

    def load_task_context(self, event_header):   # 加载task前调用
        print('hook_load_task_context success')
        print(event_header)           # {'message_id': b'7e167ce3f5a0471ea99bdd647c4b797b', 'v': 3}
     

class HelloRPC(object):
    def hello(self, name):
        """
        a test
        """
        return "Hello, %s" % name

    def howareyou(self, name):
        return "Hello are you, %s ?" % name

s = zerorpc.Server(HelloRPC())
s.debug=True
print('-->', s._events.context == s._context)  # True
print(s._methods)
print('----->', s._methods['_zerorpc_list']()[0])
print(s._methods['_zerorpc_inspect']())  # {'name': 'HelloRPC', 'methods': {'hello': {'args': [{'name': 'self'}, {'name': 'name'}], 'doc': 'a test'}}}
print(s._context.register_middleware(middleware()))
print('--------------------------')
print(s('hello', 'test __call__'))  # Hello, test __call__
s.bind("tcp://0.0.0.0:4242")
s.run()