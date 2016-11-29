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

def collect_staff_bg_erp (compids=['315', '319', '305', '320'], page=0, src='', des='', out_dir=vgvars.dir_path['out_dir'], output_name='sfaff_bg_erp'):
	###### initialize objects
	src = APIStorage(vgvars.erp) if not src else src

	apicollector = DataCollector(src,'')

	####### perform
	dept_stmt_temp = vgvars.erp['staffbg']

	if compids:
		for compid in compids:

			if not page:
				page_num = apicollector.fetch_data(dept_stmt_temp.format(compid,1,'', ''))['data']['totalPage']			
			
			# page_num = 2 # TESTING	
			datalist = []
			for n in range(1, page_num + 1):			
				data = apicollector.fetch_data(dept_stmt_temp.format(compid, n,'', ''))['data']['currentItems']
				datalist.extend(data)

			des = FileStorage({'fpath': '{}{}_{}'.format(out_dir, output_name, compid)})
			apicollector.des = des					
			apicollector.insert_data({
				'selected_format': 'json',
				'values': datalist
			})
		