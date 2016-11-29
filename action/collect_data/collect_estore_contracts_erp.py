######## import buil-in ################################################
import math, requests, copy
import datetime, logging, shelve
from collections import defaultdict
import pandas as pd
from pandas import DataFrame

######### import custom library ########################################
import vgvars 
import convert_datetime, common_lib
import collector, common_lib, convert_datetime
from storage import DBStorage,GSStorage,FileStorage,VarStorage,APIStorage
from collector import DataCollector,VGDBCollector
from sqlObject import SQLObject,VGSQL 

######### import other custom library ####################################


######### define functions ##############################################

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def group_contract_by_start_date(storage, contracts):
	num = len(contracts)
	for i in range(num):
		c = contracts[i]
		start_date = c['createdDateTime'].split(' ')[0]
		start_month = '-'.join(start_date.split('-')[:2])
		storage[start_month].append(c)

def collect_estore_contracts_erp(src='', des='', contract_ids=[], from_date='', serviceName='', page=0, out_dir=vgvars.dir_path['out_dir'], output_name='e_erp_contract'):

	###### initialize objects
	src = APIStorage(vgvars.erp) if not src else src
	des = VarStorage({'data': defaultdict(list)}) if not des else des
	apicollector = DataCollector(src,des)	

	####### perform
	contract_stmt_temp = vgvars.erp['contract']
	contract_dict = defaultdict(list)

	if not page:
		page_num = apicollector.fetch_data(contract_stmt_temp.format(1,'',''))['data']['totalPage']

	# page_num = 2 # TESTING	

	for n in range(1, page_num + 1):
		data = apicollector.fetch_data(contract_stmt_temp.format(n,'',''))['data']['currentItems']
		if from_date:
			df = DataFrame(data)
			origin_dates = df['createdDateTime']
			df['createdDateTime'] = pd.to_datetime(df['createdDateTime'])
			selectedDf = df[df['createdDateTime'] >= from_date]
			selectedDf['createdDateTime'] = selectedDf['createdDateTime'].map(lambda x: x.strftime('%Y-%m-%d'))
			selectedDf = selectedDf.T
			selected_data = selectedDf.to_dict()
			
			group_contract_by_start_date(contract_dict, selected_data)
			if len(selected_data) < len(data): break

		else:
			group_contract_by_start_date(contract_dict, data)

	for m in contract_dict:
		apicollector.des = FileStorage({'fpath': '{}{}_{}'.format(out_dir, output_name, m)})
		apicollector.insert_data({
			'selected_format': 'json',
			'values': contract_dict[m]
		})