#######
import logging,json
import math

#######
import pandas as pd, numpy as np
from pandas import DataFrame, Series
from matplotlib import pyplot as plt

#######

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class XDHelper():
	def get_utilities(self):
		print('''*** Utilities:
. get_csvdfs
. get_count
. get_counts''')		

	def get_orders(self):
		print('''*** Functions to analyze order data:
. get_ratio_succ_enum_to_no_succ_enum
. plot_order_counts''')

# display boxplot of each column in a DF
def boxplot(df):
	fig, ax = plt.subplots()
	df.boxplot(column=list(df.columns), ax=ax)
	plt.show()
	
def get_jsondfs(filenames):
	dfs = []
	fnum = len(filenames)
	for i in range(fnum):
		dfs.append(DataFrame(json.loads(list(open(filenames[i]))[0])))
	return dfs	


def get_csvdfs(filenames, header=0):
	dfs = []
	fnum = len(filenames)
	for i in range(fnum):
		dfs.append(pd.read_csv(filenames[i], header=header))
	return dfs

# get count of an attribute from a DF
# get DF from csv files that are similar characteristic
def get_count(df, name):
	attr = df[name]
	return attr.value_counts()

# get counts of same attributes from different DF, and concate them together
def get_counts(dfs, name):
	counts = []
	numdf = len(dfs)
	for i in range(numdf):
		counts.append(get_count(dfs[i], name))

	return counts

# assume datetime values are string 
def to_datetime(df,dname):
	dseries = df[dname]
	dseries.map(lambda x: x.split(' ')[0])
	dseries = pd.to_datetime(dseries)
	return dseries