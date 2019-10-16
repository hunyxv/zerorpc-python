import zerorpc

class middleware():
    def resolve_endpoint(self, endpoint):
        print(endpoint)
        return endpoint

    def client_before_request(self, event):
        print('client_before_request')

    def server_before_exec(self, event):
        print('server_before_exec success')
    


    def load_task_context(self, event_header):
        print('hook_load_task_context success')
        print(event_header)
     

c = zerorpc.Client()
c._context.register_middleware(middleware())
c.connect("tcp://127.0.0.1:4242")
print(c.hello("RPC"))
