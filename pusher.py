from zerorpc import Publisher
import time
publisher = Publisher()
publisher.bind('tcp://0.0.0.0:12123')

time.sleep(2)  # 要稍等一会

for name in ['abd', 'world', 'beijing']:
    print(name)
    publisher.say_hello(name)
