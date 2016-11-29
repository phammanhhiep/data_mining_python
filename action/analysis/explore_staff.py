#######
import logging
import math

#######
import pandas as pd, numpy as np
from pandas import DataFrame, Series
from matplotlib import pyplot as plt

#######
import analysis_utilities as au
import explore_contracts as xc

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

''' How?
	- merger transactions by day of each staff with their activities
'''

class StaffExplorer(xc.ConExplorer):
	def __init__(self, bgnames='', trx='', bg=DataFrame()): 
		if bgnames:
			self.bg = au.get_jsondfs(bgnames)
			self.bg = pd.concat(self.bg)
		else:
			self.bg = bg
		self.trx = trx

