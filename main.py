class Config:
	adress = '127.0.0.1'
	queue_name = 'taigaQueue'
class Consummer:
	@classmethod
	def callback(cls,ch, method, properties, body):
		str_body = body.decode()
		print(str_body)
		
	def __call__(self, *args, **kwargs):
		import pika
		
		connection = pika.BlockingConnection(pika.ConnectionParameters(host=Config.adress))
		
		channel = connection.channel()
		channel.queue_declare(queue=Config.queue_name)
		
	
		channel.basic_consume(Config.queue_name,Consummer.callback,  auto_ack=True)
		
		channel.start_consuming()
		
		

if __name__ == "__main__":
	
	consummer= Consummer()
	consummer()