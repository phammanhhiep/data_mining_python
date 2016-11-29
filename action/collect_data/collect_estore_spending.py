######## import buil-in ################################################
import math, requests
import datetime, logging, shelve
from collections import defaultdict

######### import custom library ########################################
import vgvars 
import convert_datetime, common_lib
import collector, common_lib, convert_datetime
from storage import DBStorage,GSStorage,FileStorage,VarStorage
from collector import DataCollector,VGDBCollector
from sqlObject import SQLObject,VGSQL 

######### import other custom library ####################################
from collect_orders import collect_estore_ids

######### define functions ##############################################

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# not include testing orders
def get_estore_id_stmt(start_id, end_id, init_stmt={}):
	orders_checked_sql.add_select([
		'orders_new.ord_estore_id'
	])

	# add from clause here
	orders_checked_sql.add_from([
		'orders_new',
		'INNER JOIN orders_new_checked on orders_new.ord_id = orders_new_checked.onc_order_id',		
	])	

	# add more where clause
	orders_checked_sql.add_where([
		'orders_new_checked.onc_status NOT IN (35,45)',
		'(orders_new.ord_id between {0} and {1})'.format(start_id, end_id),
	])	

	orders_checked_sql.gen_stmt()

	return orders_checked_sql.stmt

# generate stmt to query estore spending in a period of time and of given estores
def get_collect_estore_spending_stmt(start_id, end_id, table_num, estore_ids=[], init_stmt={}):
	esp_sql = VGSQL(init_stmt)

	esp_sql.add_select([
		'user_spent_{0}.us_user_id'.format(table_num),
		'user_spent_{0}.us_total_money'.format(table_num),
		'FROM_UNIXTIME(user_spent_{0}.us_date)'.format(table_num),
		'user_spent_{0}.us_type'.format(table_num),
		'user_spent_{0}.us_is_km'.format(table_num),
		'user_spent_{0}.us_is_km'.format(table_num),
		'user_spent_{0}.us_source'.format(table_num),
	])

	esp_sql.add_from([
		'user_spent_{0} INNER JOIN users ON users.use_id = user_spent_{0}.us_user_id'.format(table_num), 
	])

	esp_sql.add_where([
		# 'users.use_active = 1', # no need estores to be active
		'(user_spent_{0}.us_id BETWEEN {1} AND {2})'.format(table_num,start_id, end_id),
	])

	if estore_ids:
		esp_sql.add_where([
			'user_spent_{0}.us_user_id IN ({1})'.format(table_num, estore_ids)
		])
	else:  
		esp_sql.add_where([
			'users.use_supplier = 1 ', # if estore ids are not supplied, need to make sure that user is supplier
		])	

	esp_sql.gen_stmt()

	return esp_sql.stmt

# data: spending, estore, service
# operation: get spending of given estores and within a period of time
def collect (start_date, end_date, collector, estore_ids=[], user_spent_table_num=100, gen_stmt=''):

	estore_spending_mapping = defaultdict(str) if not estore_ids else collector.resolve_hashing_tables(estore_ids, user_spent_table_num)	

	# for each user_spent table
	for n in range(user_spent_table_num):
		target_estore_ids = estore_spending_mapping[n]

		if target_estore_ids:
			target_estore_ids = ','.join([str(eid) for eid in target_estore_ids])

		min_id, max_id = collector.get_max_min_id_by_time(
			start_date, 
			end_date, 
			target_id_name = 'us_id', 
			target_table_name = 'user_spent_{}'.format(n), 
			date_attr_name = 'us_date',
		)			

		# not id meets requirements
		if not min_id or not max_id:
			continue

		# creata a bin of estore ids	
		us_id_bins= collector.create_id_bucket(min_id, max_id)

		# for each group of user spent group
		for us_ids in us_id_bins:
			start_id = us_ids[0]
			end_id = us_ids[1]
			stmt = gen_stmt(start_id, end_id, n, target_estore_ids)
			logging.debug('collect spending uid {} - {} - table {}'.format(start_id, end_id, n,))
			collector.fetch_data(stmt)
			collector.insert_data()


def collect_estore_spending (start_date, end_date, eids=[], get_eids=False, src='', des='', out_dir=vgvars.dir_path['out_dir'], output_name='e_spending_data', gen_stmt=get_collect_estore_spending_stmt):
	###### prepare
	has_des = False if not des else True

	###### initialize objects
	src = DBStorage(vgvars.vgdb) if not src else src
	db_collector = VGDBCollector(src)

	###### collecting 
	# make sure that data is collect within a selected period of time like a year, a month, or a week 
	date_list = convert_datetime.divide_dates(start_date, end_date) # in months

	for d in date_list:
		start_date, end_date = d

		# update destination storage
		# des = GSStorage(des)
		des = des if has_des else FileStorage({'fpath': '{}{}_{}_{}'.format(out_dir, output_name, start_date, end_date)})
		db_collector.des = des

		# not good enough, since data is check against all estores having orders in a month
		if get_eids:
			min_ord_id, max_ord_id = db_collector.get_max_min_id_by_time(start_date, end_date)
			eids = collect_estore_ids(min_ord_id, max_ord_id, db_collector)

		# collect data
		collect(start_date, end_date, db_collector, eids, gen_stmt=gen_stmt)

	return db_collector.des.data	 

