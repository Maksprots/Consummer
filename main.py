import pika
import json
class Config:
	adress = 'localhost'
	queue_name = 'Main_queue2'


class Consummer:
	@classmethod
	def parsing_message(cls, ch, method, properties, message):
		message_json = json.loads(message.decode())
		
		print(message_json)
	
	
	
	
	def __call__(self, *args, **kwargs):
		
		
		connection = pika.BlockingConnection(pika.ConnectionParameters(host=Config.adress))
		
		channel = connection.channel()
		#channel.queue_declare(queue=Config.queue_name)
		
		channel.basic_consume(Config.queue_name, Consummer.parsing_message, auto_ack=False)
		
		channel.start_consuming()


if __name__ == "__main__":
	consummer = Consummer()
	consummer()
