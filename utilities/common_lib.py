import convert_datetime, datetime, csv, logging, json, shelve, os
# logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def add_to_shelve(db_name, key, data):
	with shelve.open(db_name) as db:
		db[key] = data

def gen_quarter_by_date(date_str):
	split_time = date_str.split('-')
	month = int(split_time[1])
	if month <= 3:
		str_month = '{}-3-2-1'
	elif month > 3 and month <= 6:
		str_month = '{}-6-5-4'
	elif month > 6 and month <= 9:
		str_month = '{}-9-8-7'
	else:
		str_month = '{}-12-11-10'
	return str_month.format(split_time[0])

def is_shelve_exist(db_name):
	with shelve.open(db_name) as db:
		for k in db:
			return True
	return False


def read_csv(file_name, extracted=True):
	# require import csv
	if extracted:
		with open(file_name, 'r', newline='', encoding='utf-8') as fd:
			r = csv.reader(fd, delimiter=',')
			result = [line for line in r]
			return result
	else:
		fd = open(file_name, 'r', newline='', encoding='utf-8')
		r = csv.reader(fd, delimiter=',')
		return r		

def read_text(file_name):
	with open(file_name, 'r', encoding='utf-8') as fd:
		data = [line for line in fd]
		return data	


def singleton(cls):
	instances = {}
	def wrapper(config):
		sheet_name = config['sheet_name']
		if not instances.get(sheet_name, ''):
			instances[sheet_name] = cls(config)
			return instances[sheet_name]
	return wrapper

def write_text(file_name, data, mode='w'):
	with open(file_name, mode, encoding='utf-8') as fd:
		fd.write(data)

def write_csv(file_name, data, mode='w', convert_to_list=False):
	# require import csv
	with open(file_name, mode, encoding='utf-8', newline='\n') as fd:
		w = csv.writer(fd, delimiter=',')
		w.writerows(data)


	
def write_json(file_name, data):
	with open(file_name, 'w') as fd:
		json.dump(data, fd)

def write_shelve(db_name, data):
	with shelve.open(db_name) as db:
		for k in data:
			db[k] = data[k]
	return db_name	


