import zerorpc

class middleware():
    def resolve_endpoint(self, endpoint):
        print(endpoint)
        return endpoint

    def client_before_request(self, event):
        print('client_before_request')

    def server_before_exec(self, event):
        print('server_before_exec success')
    


    # def load_task_context(self, event_header):
    #     print('hook_load_task_context success')
    #     print(event_header)
        

class HelloRPC(object):
    def hello(self, name):
        return "Hello, %s" % name

s = zerorpc.Server(HelloRPC())
s.debug=True
print(s._context.register_middleware(middleware()))
s.bind("tcp://0.0.0.0:4242")
s.run()