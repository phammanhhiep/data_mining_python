####### import built-in packages ##########
import requests,datetime, math, logging
from abc import ABCMeta, abstractmethod
from collections import defaultdict	
# ######## import custom package ###########
import vgvars
import storage,convert_datetime
from sqlObject import VGSQL

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# delegate requests to Storage to collect data and insert data
class Collector(metaclass=ABCMeta):
	def __init__(self): pass

	@abstractmethod
	def fetch_data(self): pass

	@abstractmethod
	def insert_data(self): pass


# consider to use factory patter to make source and des object creation transparent 
# collector instance does not care about which type of source and destination it receive. 
# the only requirement is that they must have the expected interface
class DataCollector(Collector):	
	def __init__(self, s='', d=''):
		self.src = s
		self.des = d	
		self.data = []

	def fetch_data(self,config={}):
		self.data = self.src.fetch(config)
		return self.data

	def insert_data(self, config=''):	
		if not config:
			config = {
				'values': self.data
			}

		if not config.get('values', ''):
			config['values'] = self.data
	
		self.des.insert(config)	


# Represent vg database
class VGDBCollector(DataCollector):
	def __init__(self, s='',d=''): 
		self.src = s
		self.des = d
	

	# id_attr should have similar characteristics as orders_new.ord_id: incremented and increasing overtime
	# apply to table that has id increase over time
	def get_max_min_id_by_time(self, start_date, end_date, target_id_name = 'ord_id', target_table_name = 'orders_new', date_attr_name = 'ord_date', conditions=[]):
		# can be order_id
		# value must be a squence and incremented 	
		##### validate date
		start_date_end = start_date + ' 23:59:59' 
		today_str = convert_datetime.datetime_to_string(datetime.datetime.now())
		if convert_datetime.compare_string_of_date(end_date, today_str):
			end_date = today_str
			
		end_date_end = end_date + ' 23:59:59'

		min_sql = VGSQL()
		max_sql = VGSQL()		

		min_sql.add_select([
			'min({0})'.format(target_id_name), 
		])

		min_sql.add_from([
			target_table_name,
		])

		min_sql.add_where([
			'{0} >= unix_timestamp("{1}") AND {0} <= unix_timestamp("{2}")'.format(date_attr_name, start_date,start_date_end),
		])

		max_sql.add_select([
			'min({0})'.format(target_id_name), 
		])

		max_sql.add_from([
			target_table_name,
		])

		max_sql.add_where([
			'{0} >= unix_timestamp("{1}") AND {0} <= unix_timestamp("{2}")'.format(date_attr_name, end_date, end_date_end),
		])		

		if conditions:
			min_sql.add_where(conditions)			
			max_sql.add_where(conditions)	

		min_sql.gen_stmt()
		max_sql.gen_stmt()	

		##### fetch
		min_id = self.fetch_data(min_sql.stmt)
		max_id = self.fetch_data(max_sql.stmt)

		if (min_id and max_id) and (min_id[0][0] and max_id[0][0]):
			# print(min_id,max_id)
			self.min_id = min_id[0][0]
			self.max_id = max_id[0][0]
		else:
			self.min_id = None
			self.max_id = None

		return self.min_id, self.max_id


	# Gen a dict, whose key is the number of orders_product_x, and values are corresponding estore ids
	def resolve_hashing_tables (self, ids, table_num):
		m = defaultdict(list)

		for i in ids:
			n = i % table_num
			m[n].append(i)

		return m
	
	# used to split sequence of id values into smaller groups
	# e.g: order id, or user id  
	def create_id_bucket (self, min_id='', max_id='', max_member_num = vgvars.max_vgdb_query_num):
		if not min_id or not max_id:
			min_id = self.min_id
			max_id = self.max_id 	

		group_number = math.floor((max_id - min_id) / max_member_num)

		id_groups = []
		start_id = 0 # placeholder
		next_id = 0 # placeholder

		for i in range(group_number):
			start_id = self.min_id if i == 0 else start_id + max_member_num
			next_id = start_id + max_member_num - 1	
			id_groups.append((start_id,next_id))

		# if the difference bwt max and min < max_member_num
		if not next_id:
			start_id = min_id
			next_id = max_id
			id_groups.append((start_id,next_id))
	
		if next_id < max_id:
			start_id = start_id + max_member_num
			next_id = start_id + (max_id - next_id) - 1
			id_groups.append((start_id,next_id))

		return id_groups	