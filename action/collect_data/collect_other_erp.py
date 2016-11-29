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

# collect company info of vnp
def collect_company_bg(src='', des='', out_dir=vgvars.dir_path['out_dir'], output_name='company_erp'):

	###### initialize objects
	src = APIStorage(vgvars.erp) if not src else src
	# des = VarStorage({'data': defaultdict(list)}) if not des else des
	des = FileStorage({'fpath': '{}{}'.format(out_dir, output_name)})
	apicollector = DataCollector(src,des)	

	####### perform
	company_stmt_temp = vgvars.erp['company']

	data = apicollector.fetch_data(company_stmt_temp)['data']

	apicollector.insert_data({
		'selected_format': 'json',
		'values': data
	})	

# get department bg of a company in vnp
# company_id can be 315 (vg ha noi) or 319 (vg hcm). For other companies, collect_comp_bg
def collect_dept_bg(company_id=vgvars.erp_default['vghn'], src='', des='', out_dir=vgvars.dir_path['out_dir'], output_name='dept_erp'):

	###### initialize objects
	src = APIStorage(vgvars.erp) if not src else src
	# des = VarStorage({'data': defaultdict(list)}) if not des else des
	des = FileStorage({'fpath': '{}{}_{}'.format(out_dir, output_name, company_id)})
	apicollector = DataCollector(src,des)	

	####### perform
	dept_stmt_temp = vgvars.erp['dept']

	data = apicollector.fetch_data(dept_stmt_temp.format(company_id))['data']

	apicollector.insert_data({
		'selected_format': 'json',
		'values': data
	})


def collect_customer_bg(cusid='', page=0, src='', des='', out_dir=vgvars.dir_path['out_dir'], output_name='customer_erp'):
	###### initialize objects
	src = APIStorage(vgvars.erp) if not src else src
	# des = VarStorage({'data': defaultdict(list)}) if not des else des
	# des = FileStorage({'fpath': '{}{}_{}'.format(out_dir, output_name)})
	apicollector = DataCollector(src, des)	

	customer_stmt_temp = vgvars.erp['customer']

	if cusid: 
		customer_stmt_temp =  customer_stmt_temp.format('id={}'.format(cusid))
		data = apicollector.fetch_data(customer_stmt_temp)['data']['currentItems']
		des = FileStorage({'fpath': '{}{}_cusid_{}'.format(out_dir, output_name, cusid)})
		apicollector.des = des
		apicollector.insert_data({
			'selected_format': 'json',
			'values': data
		})
	else:
		customer_stmt_temp = customer_stmt_temp.format('page={}')
		if not page:
			page_num = apicollector.fetch_data(customer_stmt_temp.format(1))['data']['totalPage']	
		
		# page_num = 2 # Testing	

		for i in range(1, page_num + 1):	
			data = apicollector.fetch_data(customer_stmt_temp.format(i))['data']['currentItems']
			des = FileStorage({'fpath': '{}{}_{}'.format(out_dir, output_name, i)})			
			apicollector.des = des
			apicollector.insert_data({
				'selected_format': 'json',
				'values': data
			})

def collect_product_bg(): pass
