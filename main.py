import pika
import json

class Config:
	adress = 'localhost'
	queue_name = 'Main_queue2'
	path_to_outfile_with_closed_tasks=r'C:\Users\maksl\Desktop\consumer\closed_tasks.txt'


class Consummer:
	@classmethod
	def parsing_message(cls, ch, method, properties, message):
		message_json = json.loads(message.decode())

		
		if message_json["type"] == "task" and message_json['data']['status']['is_closed']   :
			# если сообщение про таск и таск закрыли
			with open(Config.path_to_outfile_with_closed_tasks, 'r+') as closed_tasks:
				into_file = closed_tasks.read()
				into_file+=into_file + str(message_json)
				closed_tasks.write(into_file)
				closed_tasks.close()
				
				
				
				
				
		
		
	
	
	
	
	def __call__(self, *args, **kwargs):
		
		
		connection = pika.BlockingConnection(pika.ConnectionParameters(host=Config.adress))
		
		channel = connection.channel()
		
		
		channel.basic_consume(Config.queue_name, Consummer.parsing_message, auto_ack=False)
		
		channel.start_consuming()


if __name__ == "__main__":
	consummer = Consummer()
	consummer()
