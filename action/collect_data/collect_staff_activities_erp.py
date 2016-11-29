######## import buil-in ################################################
import math, requests
import datetime, logging, shelve
from collections import defaultdict

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

def group_activities_by_date(storage, act):
	num = len(act)
	for i in range(num):
		c = act[i]
		start_date = c['createdDate']
		start_month = '-'.join(start_date.split('-')[:2])
		storage[start_month].append(c)	

		for k in storage:
			if k != start_month:
				return k
	return ''			

# activities of all staff, including sales and customer services
def collect_staff_activities_erp(src='', des='', activity_type='', from_date='', page=0, out_dir=vgvars.dir_path['out_dir'], output_name='staff_activities'):
		###### initialize objects
	src = APIStorage(vgvars.erp) if not src else src
	# des = VarStorage({'data': defaultdict(list)}) if not des else des
	des = FileStorage({'fpath': '{}{}'.format(out_dir, output_name)})
	apicollector = DataCollector(src,des)	

	####### perform
	activities_stmt_temp = vgvars.erp['activities']
	activities_dict = defaultdict(list)
	if not page:
		page_num = apicollector.fetch_data(activities_stmt_temp.format(1,'',''))['data']['totalPage']

	# page_num = 2 # TESTING	

	for n in range(1, page_num + 1):
		data = apicollector.fetch_data(activities_stmt_temp.format(n,'',''))['data']['currentItems']
		if from_date:
			df = DataFrame(data)
			origin_dates = df['createdDate']
			df['createdDate'] = pd.to_datetime(df['createdDate'])
			selectedDf = df[df['createdDate'] >= from_date]
			selectedDf['createdDate'] = selectedDf['createdDate'].map(lambda x: x.strftime('%Y-%m-%d'))
			selectedDf = selectedDf.T
			selected_data = selectedDf.to_dict()
			
			inserting = group_activities_by_date(activities_dict, selected_data)

			if len(selected_data) < len(data): break

		else:
			inserting = group_activities_by_date(activities_dict, data)

		if inserting:
			apicollector.des = FileStorage({'fpath': '{}{}_{}'.format(out_dir, output_name, inserting)})
			apicollector.insert_data({
				'selected_format': 'json',
				'values': activities_dict[inserting]
			})	

			del activities_dict[inserting]		
	
	if len(activities_dict):
		for k in activities_dict:
			apicollector.des = FileStorage({'fpath': '{}{}_{}'.format(out_dir, output_name, k)})
			apicollector.insert_data({
				'selected_format': 'json',
				'values': activities_dict[k]
			})	
			