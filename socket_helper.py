import json
import pika
import uuid
from abc import ABCMeta, abstractmethod

class SocketHelper(object):
	__metaclass__ = ABCMeta
	def __init__(self, import_name):
		super(SocketHelper, self).__init__(import_name)
		self.channel = ''
		self.secret_key = 'A0Zr98j/3yX R~XHH!jmN]LWX/,?RT'
		self.responses = {}
		
		self.connection = pika.BlockingConnection(pika.ConnectionParameters(
                host='localhost'))

        self.channel = self.connection.channel()

        result = self.channel.queue_declare(exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(self.on_response, no_ack=True,
                                   queue=self.callback_queue)
								   
		
	
	def on_request(ch, method, props, body):
	
		json_data = json.loads(body)
		response = self.process_response(json_data['data'])

		ch.basic_publish(exchange='',
						 routing_key=props.reply_to,
						 properties=pika.BasicProperties(correlation_id = \
														 props.correlation_id),
						 body=str(response))
		ch.basic_ack(delivery_tag = method.delivery_tag)
	
	def receive_request(self):
		self.channel.basic_qos(prefetch_count=1)
		self.channel.basic_consume(self.on_request, queue=self.channel)
		self.channel.start_consuming()
	
	def on_response(self, ch, method, props, body):
		lock = threading.RLock()
		with lock:
			self.responses[props.correlation_id] = body
		
	def send_message(self, channel, json_data):
		
		# send data
		corr_id = str(uuid.uuid4())
		self.channel.basic_publish(exchange='',
                                   routing_key = channel,
                                   properties=pika.BasicProperties(
                                         reply_to = self.callback_queue,
                                         correlation_id = corr_id,
                                         ),
                                   body=json.dumps(json_data))
        while corr_id not in self.responses is None:
            self.connection.process_data_events()
		
		returned_data = self.responses[corr_id]
		
		lock = threading.RLock()
		with lock:
			del self.responses[corr_id]
		
        return returned_data
		
		
	@abstractmethod
	def process_response(self, json_data):
		return NotImplemented