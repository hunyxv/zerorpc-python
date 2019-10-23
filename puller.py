from zerorpc import Subscriber



class Hello():
    def say_hello(self, name):
        print("hello, %s." % name)



subscriber = Subscriber(Hello())
subscriber.connect('tcp://0.0.0.0:12123')

subscriber.run()