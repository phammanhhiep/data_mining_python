#######
import logging
import math
#######
import pandas as pd, numpy as np
from pandas import DataFrame, Series
from matplotlib import pyplot as plt
#######
import vgvars
#######
import explore_contracts as xc

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

''' get trend of transaction amount and numbers each dates of a month
	+ visualize: 
		- plot histogram of all dates, all week, all months, all quarter, each day of weeks, 
		- plot lines of all dates, all week,  all months, all quarter, each day of weeks,
		- plot boxplot all dates, all week, all months, all quarter, each day of weeks, each month, 

	+ statistics:
		- 5 summary number
		- mean and SD if necessary
		- relationship 

	+ other:
		- calculate culmulative revenue each days of a week and a month, each week of a month
		- calculate culmulative count of transactions each days of a week and a month, each week of a month	
		- how to increase revenue or change the spread of revenue of day of a month
'''

''' Question:
	+ count by days, week, month, quarter, and year, days of week, weeks of month, months of quarter
	+ trends? contribution each a period of time to the whole time?
'''

def main():

	out_dir = '/home/hieppm/git_repos/out/data_mining_python/collect_data/'

	contract2016names = ['e_erp_contract_2016-01','e_erp_contract_2016-02','e_erp_contract_2016-03','e_erp_contract_2016-04','e_erp_contract_2016-05','e_erp_contract_2016-06','e_erp_contract_2016-07','e_erp_contract_2016-08','e_erp_contract_2016-09','e_erp_contract_2016-10', 'e_erp_contract_2016-11']

	contract2016names = [out_dir + i for i in contract2016names]

	trx = xc.TranExplorer(contract2016names)	
	reportCalls = {
		'-1': exit,
		'0': lambda x: print('Do nothing'),

	}


	response = '0'
	while response != -1:
		reportCalls[response]('')
		print('''Select a report:
			'-1': exit
			'0': Do nothing

		''')
		response = input(''' 

		''')

