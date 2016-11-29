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

# Assume: only take use either estore_loginname or estore_ids, not both.
# estore_ids is a list of strings
def get_collect_estore_bg_stmt(estore_loginnames='', estore_ids='', start_date='', init_stmt={}):
	esp_sql = VGSQL(init_stmt)

	esp_sql.add_select([
		'users.use_id',
		'users.use_loginname',
		'users.use_active',
		'users.use_group', # admin (14), sales (16), estores
		'users.use_supplier',
		'users.use_estore_transport',
		'users.use_phone',
		'users.use_email',
		'users.use_skype',
		'users.use_province',
		'countries.cou_short_name', # name of the province or city
		'users.use_address',
		'users.use_date', # joinning data
		'users.use_verified', 

		'users.use_verify_type', # dont know usage
		'users.use_estore', # dont know usage
		'users.use_estore_temp', # dont know usage
		'users.use_protect', # dont know usage
		'users.use_spammer', # dont know usage

		'users.use_company',
		'users.use_website',	
		'users.use_estore_temp',
		'users.use_loginphone',
	])

	esp_sql.add_from([
		'users',
		'LEFT JOIN countries ON users.use_province = countries.cou_id', 
	])


	esp_sql.add_where([
		'users.use_group NOT IN (14,16)',
		# 'users.use_supplier = 1', # not use the condition by default since many estore can be banned and their accounts can be modified as a customer accounts. No need such condition if already has estore ids  
		# 'users.use_active = 1', # no need estores to be active
	])


	if start_date:
		esp_sql.add_where([
			'users.use_date > {}'.format(start_date),
			# 'users.use_active = 1', # no need estores to be active
		])

	if estore_loginnames:
		estore_loginnames = ','.join(['"{}"'.format(i) for i in estore_loginnames])
		esp_sql.add_where([
			'users.use_loginname IN ({})'.format(estore_loginnames)
		])

	if estore_ids:
		esp_sql.add_where([
			'users.use_id IN ({})'.format(','.join([str(e) for e in estore_ids]))
		])			

	esp_sql.gen_stmt()
	return esp_sql.stmt

# collect estore bg who having orders in a given period of time
def get_ordered_estore_id(start_date, end_date, collector):
	return collect_estore_ids(collector=collector, start_date=start_date, end_date= end_date, query_limited=True) 


# Collect background of estores
# Pass either estore ids or loginnames to collect data.  
def collect_estore_bg(eids=[], eloginnames=[], get_eids=False, get_eids_args=[], get_eids_function=get_ordered_estore_id, src='', des='', out_dir=vgvars.dir_path['out_dir'], output_name='e_bg', gen_stmt=get_collect_estore_bg_stmt, max_query=vgvars.max_vgdb_query_num):

	###### prepare
	has_des = False if not des else True

	###### initialize objects
	src = DBStorage(vgvars.vgdb) if not src else src
	db_collector = VGDBCollector(src)

	# by default, collect estore bg of estore that has order in a given period of time
	if get_eids:
		get_eids_args.append(db_collector) 
		eids = get_eids_function(*get_eids_args)

	if eloginnames or eids: 
		if eloginnames:
			qnum = math.ceil(len(eloginnames) / max_query)
		else:
			qnum = math.ceil(len(eids) / max_query)
				
		for i in range(qnum):

			start_index = i * max_query
			end_index = start_index + max_query

			selectedloginnames = eloginnames[start_index:end_index]
			selectedeids = eids[start_index:end_index]

			des = des if has_des else FileStorage({'fpath': '{}{}_{}_{}'.format(out_dir, output_name, start_index, end_index-1)})
			db_collector.des = des

			if selectedeids or selectedloginnames:
				logging.debug('collect estore bg from {} to {}'.format(start_index,end_index-1))
				stmt = get_collect_estore_bg_stmt(estore_loginnames=selectedloginnames, estore_ids=selectedeids,)
				db_collector.fetch_data(stmt)
				db_collector.insert_data()

	else: # later. collect bg when no eids and eloginnames provided
		pass







