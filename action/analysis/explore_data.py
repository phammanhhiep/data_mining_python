#######
import logging
import math

#######
import pandas as pd, numpy as np
from pandas import DataFrame, Series
from matplotlib import pyplot as plt

#######
import analysis_utilities as au

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

################################################ ESTORES

# get groupby object of root cat numbers of estores
def get_root_cat_number_of_estores (df, ename='ord_estore_id', rcatname='cat_root'):
	cr = df[[ename, rcatname]]
	grouped = cr.groupby(ename)
	gc = grouped.apply(lambda x: len(x['cat_root'].unique()))
	return gc

# get ratio of root cat with greatest order numbers to total number of orders for each ordered estore
def get_pct_main_root_cat_of_estores (df, ename='ord_estore_id', rcatname='cat_root'):
	rcat = df[rcatname]
	grouped = rcat.groupby(df[ename])
	maxpct = grouped.apply(lambda x: (x.groupby(rcat).count().max() / x.groupby(rcat).count().sum()) if x.groupby(rcat).count().sum() else 0)
	return maxpct

# 
def get_nan_root_cat_num__of_estores(): pass 


################################################ ORDERS

# get attr count by day of week
# assume original date is in string format of a datetime
def get_order_count_by_dow(df, name, dname):
	attr = df[name]
	dates = pd.to_datetime(df[dname].map(lambda x: x.split(' ')[0]))
	dow = [d.dayofweek for d in dates]
	dates = DataFrame(dates)
	dates[date] 
	# STOP here

# ratio of number of estores with at least one succ order to that of estores with no succ orders 
def get_ratio_succ_enum_to_no_succ_enum(df, ecol='ord_estore_id', ord_status_col='onc_status'):
	estores = df[[ecol, ord_status_col]] # get estores, and their status
	grouped = estores.groupby(df[ecol]) # group by estores
	test_no_succ_ord = lambda x: sum(x[ord_status_col] != 10) == len(x) # determine which estore has not succ order. no succ orders if True
	tested_estores = grouped.agg(test_no_succ_ord) # get a DF with estore ids and estores' new status
	tested_estores = tested_estores.ix[:,0] # get the Series from its DataFrame
	
	nso_e_num = (tested_estores == True).sum() # no-succ estore num 
	so_e_num = len(tested_estores) - nso_e_num # succ estore num

	return so_e_num / nso_e_num

def get_total_ordered_estore_num(df): pass 

# assumptions: 
#	. dfs have the same attributes in different periods of time
#	. concatenate by the selected attr
#	. number of unique attribute values should be small
def plot_order_counts(dfs, name, transpose=False):
	cs = au.get_counts(dfs, name)
	concatedcs = pd.concat(cs, axis=1)
	concatedcs.transpose().plot()
	plt.show()


################################################## PRODUCTS


################################################## CUSTOMERS


################################################### STAFF






