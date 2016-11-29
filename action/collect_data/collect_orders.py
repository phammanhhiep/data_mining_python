
######## import buil-in ################################################
import math, requests, logging
import datetime, shelve, copy
from collections import defaultdict

######### import custom: used when called from another file ############
import vgvars
import convert_datetime, common_lib
import collector
import common_lib, convert_datetime
from storage import DBStorage,GSStorage,FileStorage,VarStorage
from collector import DataCollector,VGDBCollector
from sqlObject import SQLObject,VGSQL 

######### define functions ##############################################

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# functionalities: reiceive arguments and return requested query stmt 
def get_collect_order_stmt (start_id, end_id, estore_ids='', table_num='', init_stmt={}):
	orders_checked_sql = VGSQL(init_stmt)

	orders_checked_sql.add_select([
		'orders_new.ord_id', 'orders_new.ord_estore_id', 'orders_new.ord_user_id',
		'orders_new.ord_code','FROM_UNIXTIME(orders_new.ord_date)',
		'orders_new.ord_total_money',' orders_new.ord_discount', 'orders_new.ord_shipping_cost', 
		'orders_new.ord_payment_method','orders_new.ord_payment_method_baokim', 
		'orders_new.ord_shipping_cost_type', 'orders_new.ord_delivery_method', 
		'orders_new.ord_is_mobile', 'orders_new.ord_gender', 'orders_new.ord_email', 
		'orders_new.ord_phone','orders_new.ord_city', 'orders_new.ord_district', 
		'orders_new.ord_email_baokim','orders_new.ord_estore_depot',
		'orders_new_checked.onc_status',
		'orders_referer.orr_source_referer', # the first record on 2015-08-20		
		'orders_product_{0}.op_product_id'.format(table_num),
		'orders_product_{0}.op_quantity'.format(table_num),
		'orders_product_{0}.op_price'.format(table_num),
		'products_multi.pro_category', 
		'categories_multi.cat_root',
	])

	# add from clause here
	orders_checked_sql.add_from([
		'orders_new',
		'INNER JOIN orders_new_checked on orders_new.ord_id = orders_new_checked.onc_order_id',
		'LEFT JOIN orders_referer on orders_referer.orr_order_id = orders_new.ord_id', # the first record on 2015-08-20		
		'INNER JOIN orders_product_{0} ON orders_product_{0}.op_order_id = orders_new.ord_id'.format(table_num),
		'LEFT JOIN products_multi ON orders_product_{0}.op_product_id = products_multi.pro_id'.format(table_num),
		'LEFT JOIN categories_multi ON categories_multi.cat_id = products_multi.pro_category',		
	])	

	# add more where clause
	orders_checked_sql.add_where([
		'orders_new_checked.onc_status NOT IN (35,45)',
		'(orders_new.ord_id between {0} and {1})'.format(start_id, end_id),
	])	

	if estore_ids:
		orders_checked_sql.add_where([
			'orders_new.ord_estore_id IN ({0})'.format(','.join([str(i) for i in estore_ids])),
		])

	orders_checked_sql.gen_stmt()
	return orders_checked_sql.stmt

# return estore id of estore that has orders in the period of time	
def collect_estore_ids(start_id='', end_id='',collector='', start_date='', end_date='', query_limited=False):
	collector = copy.deepcopy(collector) # avoid conflict if change any attribute value of the collector
	collector.des = VarStorage()

	init_stmt_template = {
		'select':[
			'distinct orders_new.ord_estore_id', 
		],
		'from':[
			'orders_new INNER JOIN orders_new_checked ON orders_new.ord_id = orders_new_checked.onc_order_id',			
		],
		'where':[
			'orders_new_checked.onc_status NOT IN (35,45)',	
			'(orders_new.ord_id BETWEEN {} AND {})',		
		],
	}

	
	if not query_limited: # query all data at once
		if not start_id and start_date: # gen start_id and end_id
			start_id, end_id = collector.get_max_min_id_by_time(start_date, end_date)

		init_stmt = init_stmt_template
		init_stmt['where'][1] = init_stmt['where'][1].format(start_id, end_id)
		eid_sql = VGSQL(init_stmt)
		stmt = eid_sql.gen_stmt()

		logging.debug('collect estore id with ord_id from {} to {}'.format(start_id, end_id))

		estore_ids = collector.fetch_data(stmt)
		estore_ids = [eid[0] for eid in estore_ids]

	else: # collect small data amount and then combine together
		collector.get_max_min_id_by_time(start_date, end_date)
		order_id_bins = collector.create_id_bucket()
		
		for ord_ids in order_id_bins:
			start_id = ord_ids[0]
			end_id = ord_ids[1]

			init_stmt = copy.deepcopy(init_stmt_template) # reset and important to deep copy the dict
			init_stmt['where'][1] = init_stmt['where'][1].format(start_id, end_id)
			eid_sql = VGSQL(init_stmt)

			stmt = eid_sql.gen_stmt()

			logging.debug('collect estore id with ord_id from {} to {}'.format(start_id, end_id))

			collector.fetch_data(stmt)
			collector.insert_data() # append data	
		

		estore_ids = collector.des.data	
		estore_ids = [eid[0] for eid in estore_ids]
		estore_ids = list(set(estore_ids))

	return estore_ids

# collect order data in a given period of time
def collect (start_date, end_date, collector, estore_ids=[], order_product_table_num=20, gen_stmt=''):
	collector.get_max_min_id_by_time(start_date, end_date)

	order_id_bins = collector.create_id_bucket()

	# generate sql stmt
	for ord_ids in order_id_bins:
		start_id = ord_ids[0]
		end_id = ord_ids[1]

		# pass estore and dates to gen stmt 
		estore_ids = collect_estore_ids(ord_ids[0],ord_ids[1],collector) if not estore_ids else estore_ids

		# mapping estores and orders_product tables
		estore_op_mapping = collector.resolve_hashing_tables(estore_ids, order_product_table_num)

		for table_num in range(order_product_table_num):
			target_estore_ids = estore_op_mapping[table_num]
			if target_estore_ids:
				logging.debug('collect order from {} to {} in orders_product_{}'.format(start_id, end_id, table_num))
				stmt = gen_stmt(start_id, end_id, target_estore_ids, table_num)
				collector.fetch_data(stmt)
				collector.insert_data() # append data



# delegate request of collect orders to lower level functions
# make sure to collect in monthly
def collect_orders (start_date, end_date, eids=[], src='', des='', out_dir=vgvars.dir_path['out_dir'], output_name='order_data', gen_stmt=get_collect_order_stmt):
	###### prepare
	has_des = False if not des else True

	###### initialize objects
	src = DBStorage(vgvars.vgdb) if not src else src
	db_collector = VGDBCollector(src)

	###### collecting 
	# make sure that data is collect within a selected period of time like a year, a month, or a week 
	date_list = convert_datetime.divide_dates(start_date, end_date) # in months

	for i in date_list:
		start_date, end_date = i

		# update destination storage
		# des = GSStorage(des)
		des = des if has_des else FileStorage({'fpath': '{}{}_{}_{}'.format(out_dir, output_name, start_date, end_date)})
		db_collector.des = des

		# collect data
		collect(start_date, end_date, db_collector, eids, gen_stmt=gen_stmt)

	return db_collector.des.data

