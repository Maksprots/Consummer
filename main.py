import pika
import json
import pandas


class Config:
	adress = '127.0.0.1'
	queue_name = 'taigaQueue'
	path_to_outfile_with_closed_tasks = r'closed_tasks.csv'


class Consummer:
	@classmethod
	def parsing_message(cls, ch, method, properties, message):
		message_json = json.loads(message.decode())
		
		if message_json["type"] == "task" and message_json['data']['status']['is_closed']:
			# если сообщение про таск и таск закрыли
			try:
				output_list_with_hours = pandas.read_csv(Config.path_to_outfile_with_closed_tasks,
				                                         index_col=False)
			except:
				# если нет шаблона таблицы создаем
				columns = ['Почта', 'id', 'Задача', 'Часы']
				output_list_with_hours = pandas.DataFrame(columns=columns)
			
			added_line = [message_json['by']['email'],
			              message_json['by']['id'],
			              message_json['data']['subject'],
			              message_json['data']['custom_attributes_values']['hours']]
			
			# добавление записи о таске
			output_list_with_hours.loc[len(output_list_with_hours)] = added_line
			
			print(output_list_with_hours)
			output_list_with_hours.to_csv(Config.path_to_outfile_with_closed_tasks, index=False)
	
	def __call__(self, *args, **kwargs):
		connection = pika.BlockingConnection(pika.ConnectionParameters(host=Config.adress))
		channel = connection.channel()
		channel.basic_consume(Config.queue_name, Consummer.parsing_message, auto_ack=False)
		channel.start_consuming()

if __name__ == "__main__":
	consummer = Consummer()
	consummer()
