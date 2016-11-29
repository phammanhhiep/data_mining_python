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

################################################### Questions
'''
- get dates series:
	+ contract created dates (done)
	+ transaction datas (done)
	+ first transaction dates (done)
- get contract, transaction
	+ by product (done)
	+ by company (done)
	+ by department 
	+ by service 
	+ by estore owner
	+ in different periods of time (no need)
- count:
	+ contract (no need - too basic)
	+ contract by contract status (done)
	+ contract by createdDate (done)
	+ contract by company (done)
	+ contract by department (done)
	x contract by transation status (no need - since there may be more than one transaction each contract)
	+ contract by 0 balance; pay all fee
	+ transaction by company (done)
	+ transaction by dept (done) 
	+ transaction (no need - too basic)
	+ transaction by transaction status 
	+ transaction by salesmen

- statistics
	- 5 summary number
	- mean and sd
	- relationship statistics: 

- grapth
	+ count 
		. timeseries of transaction count in different periods of time
		. timeseries of transaction count in different teams
		. timeseries of transaction count in different location
		. timeseries of contract count in different periods of time (done)
		. timeseries of contract count in different teams  
		. timeseries of contract count in different location
	+ revenue
		. timeseries of transaction revenue in different periods of time, different teams, different location
	+ ratio
		. timeseries of verified new / verified renew in different periods of time, different teams, different location
		. timeseries of verified / other product in different periods of time, different teams, different location
		. 

'''

#################################################### PREPARATION

# explore several df of contracts at a time
# assume each df represent contracts at different time range like each month of a year
class ConExplorer:
	def __init__(self, fnames='', tfile='json', dfs=''):
		if fnames and tfile == 'json':
			self.dfs = au.get_jsondfs(fnames)
		elif fnames and tfile == 'csv':
			pass
		else:
			self.dfs = dfs

	# plot 1 or multiple columns of a df
	# assume df contain counts of some variables	
	def boxplot(self, df):
		df.boxplot(column=list(df.columns))
		plt.show()

	# percentage change of culmulative sum	
	def cumsumPctChange(self, count):
		s = count.sum()
		cs = count.cumsum()
		ptcChange = 100 * cs / s
		return ptcChange	

	# join dfs together horizontally and as columns 	
	def concatx(self, dfs, reset_index=True):
		if reset_index:
			dfs = [df.reset_index(drop=True) for df in dfs]		
		return pd.concat(dfs, axis=1, ignore_index=True)	

	# group values and count	
	def countGroup(self, df, name):
		s = df[name]
		g = s.groupby(s)
		return g.count()

	def countGroups(self, df, name1, name2):
		s = df[[name1, name2]]
		g = s.groupby([df[name1], df[name2]])
		return g.count()	

	# count by teamds and companies
	def countByTeams(self, df):
		return self.countGroups(df, 'companyId', 'departmentId')

	# def countMulByTeams(self, dfs=''):
	# 	dfs = self.dfs if not dfs else dfs
	# 	return [self.countByTeams(df) for df in dfs]				

	# count by companies	
	def countByComs(self, df):	
		return self.countGroups(df, 'companyId')

	# def countMulByComs(self, dfs=''):	
	# 	dfs = self.dfs if not dfs else dfs
	# 	return [self.countByComs(df) for df in dfs]

	# from string to date 	
	def fromStrToDate(sefl, df, name):	
		dates = df[name].map(lambda x: pd.to_datetime(x.split(' ')[0]))
		df[name + '_date'] = dates

	# get boolean value indicating whether contracts having product ids as specified	
	# assume there is only one product each contract
	def getBooleanByPros(self, df, proids=['232', '231']):
		status = []
		for p in proids:
			status.append([c[0]['productId'] == p for c in df['contractProducts']])

		statusdf = DataFrame(status)
		b = statusdf.sum() > 0
		b.index = df.index
		return b

	def getBooleanByComps(self, df, compIds=['315', '319', '305', '320']):
		status = []
		for c in compIds:
			status.append(df['companyId'] == c)

		statusdf = DataFrame(status)
		b = statusdf.sum() > 0
		b.index = df.index	
		return b	

	# get group using keys
	# return a list of groups
	def getGroups(self, grouped, keys=[]):
		keys = keys if keys else list(grouped.groups.keys())
		g = []
		for k in keys:
			g.append(grouped.get_group(k))
		return g

	# group by companies	
	def groupByComs(self, df, name='companyId'):
		return df.groupby(name)		

	# group by products
	def groupByPros(self, df, f=''):
		f1 = lambda x: df.ix[x]['contractProducts'][0]['productId'] if df.ix[x]['contractProducts'] else np.nan
		f = f if f else f1
		return df.groupby(f)

	# def groupMulByPros(self, dfs='', f=''):
	# 	dfs = self.dfs if not dfs else dfs
	# 	return [self.groupByPros(df, f) for df in dfs]	

	# group by team	
	def groupByTeams(self, df, name='departmentId'):
		return df.groupby(name)		

	# def groupMulByTeams(self, dfs='', f=''):
	# 	dfs = self.dfs if not dfs else dfs
	# 	return [self.groupMulByPros(df, f) for df in dfs]

	# return a list of data for different team
	# assume get groupby first
	# by default, get data of following companies; vghn, vghcm, ddv hn, and ddv hcm
	def getGroupByComs(self, grouped='', df='', compids=['315', '319', '305', '320']):
		grouped = grouped if grouped else self.groupByComs(df)
		return self.getGroups(grouped, compids)

	# # get data by companies from multiple DFs
	# def getMulGroupByComs(self, dfs='', compids=['315', '319', '305', '320']):
	# 	dfs = self.dfs if not dfs else dfs
	# 	return [self.getGroupByComs(df, compids) for df in dfs]

	def getGroupByTeams(): pass
	def getGroupByPros(): pass 


	# combine groups into a DataFrame, each column is a group.		
	def fromGroupsToDf(self, grouped):
		glist = []
		gnames = [i for i,g in grouped]
		gnames.sort()
		gnum = len(gnames)

		for n in range(gnum):
			agroup = grouped.get_group(gnames[n])
			glist.append(agroup)

		df = self.concatx(glist).T
		df.index = gnames
		return df 	

	def pct(self, count):
		s = count.sum()
		p = 100 * count / s
		return p

	def toInt(sefl, df, name): pass

	def toFloat(self, df, name='amount'):
		df[name] = df[name].map(float)	

# explorer of contracts
class ContractExplorer(ConExplorer):
	def __init__(self, fnames='', tfile='json', dfs='', vatgiaOnly=True):
		ConExplorer.__init__(self, fnames, tfile, dfs)
		self.cons = self.getCons(pd.concat(self.dfs, ignore_index=True)) 
		if vatgiaOnly:
			self.cons = self.getConByComps()

	def countDailyCons(self, cons=DataFrame):
		cons = self.cons if cons.empty else cons
		grouped = self.groupConByDates(cons)
		dc = grouped['id'].count()
		dc = dc.resample('D').sum()
		return dc[dc.index.weekday != 6] # remove Sunday		


	def getCons(self, df):
		# get employee id from comission
		comms = df[['commissions', 'createdByName']]
		commNum = len(comms)
		eids = []

		for n in range(commNum):
			comm = comms.ix[n]['commissions']
			ename = comms.ix[n]['createdByName'].split(' ')
			while '' in ename:
				ename.pop(ename.index(''))
			ename = ' '.join(ename)
			eid = [c['employeeId'] for c in comm if c['employeeName'] == ename]

			if len(eid):
				eids.append(eid[0])
			else:
				eids.append(None)

		df['createdById'] = eids
		df['createdDate'] = df['createdDateTime'].map(lambda x: pd.to_datetime(x.split(' ')[0]))

		return df

	def getConByComps(self, cons=DataFrame(), compIds=['315', '319', '305', '320'], reverse=False): 
		cons = self.cons if cons.empty else cons
		b = self.getBooleanByComps(cons, compIds)
		if reverse: return cons[-b]
		else: return cons[b]

	# get transaction of given product ids
	def getConByPros(self, cons=DataFrame(), proids=['232', '231'], reverse=False):
		cons = self.cons if cons.empty else cons
		b = self.getBooleanByPros(cons, proids)
		if reverse: return cons[-b]
		else: return cons[b]	

	def groupConByEmployees(self, cons=DataFrame()):
		cons = self.cons if cons.empty else cons
		return cons.groupby('createdByName')

	# group by apply date	
	def groupConByDates(self, df, dname='createdDate'):
		return df.groupby(df[dname])

	def groupConByDayMonthly(self, cons=DataFrame()):
		cons = self.cons if cons.empty else cons
		return cons.groupby([cons['createdDate'].map(lambda x: x.year), cons['createdDate'].map(lambda x: x.month)])


# explorer of transactions
# always call getTrans or getMulTrans first, then apply other methods on the return data
# explore sales departments only by default	
class TranExplorer (ConExplorer):
	def __init__ (self, fnames='', tfile='json', dfs='', vatgiaOnly=True):
		ConExplorer.__init__(self, fnames, tfile, dfs)
		self.trans = self.getTrans(pd.concat(self.dfs, ignore_index=True))
		if vatgiaOnly:
			self.trans = self.getTransByComps()

	# total number of trans of each day	in a year
	def countDailyTrans(self, trans=DataFrame()):
		trans = self.trans if trans.empty else trans
		grouped = self.groupTransByDates(trans)
		dc = grouped['id_tran'].count()
		dc = dc.resample('D').sum()
		return dc[dc.index.weekday != 6] # remove Sunday

	# total number of trans of each week in a year
	# count is the daily count
	def	countWeeklyTrans(self, dayCount=Series()):
		dc = self.countDailyTrans(self.trans) if dayCount.empty else dayCount
		return dc.resample('W', closed='right', kind='period').sum()

	# total number of trans of each month in a year
	def countMonthlyTrans(self, dayCount=Series()):
		dc = self.countDailyTrans(self.trans) if dayCount.empty else dayCount
		return dc.resample('M', closed='right', kind='period').sum()	

	# total number of trans of each quarter in a year 
	def countQuarterlyTrans(self, dayCount=Series()):
		dc = self.countDailyTrans(self.trans) if dayCount.empty else dayCount
		return dc.resample('Q', closed='right', kind='period').sum()		

	# total number of tranus of each year
	def countAnnuallyTrans(self): pass

	# get transaction in contracts. Also include non-tracsactions
	def getTrans(self, df):
		trans = df[['transactions', 'id']] # transaction and contract id
		newTrans = []
		newCIndex = []

		transNum = len(trans)

		for n in range(transNum):
			record = trans.ix[n]
			cid = record['id']
			trs = record['transactions']

			if len(trs):
				[tr.update({'id_tran': tr.pop('id')}) for tr in trs]
				[tr.update({'id': cid}) for tr in trs]
				newTrans.extend(trs)
			else:
				newTrans.extend([{'id_tran': None, 'id': cid}])

		newTrans = DataFrame(newTrans)

		newdf = df.drop(['transactions'], axis=1)
		newTrans = pd.merge(newdf, newTrans, left_on=['id'], right_on=['id'], suffixes=['', '_tran'])
		newTrans['applyDate'] = pd.to_datetime(newTrans['applyDate'])
		newTrans['createdDate'] = newTrans['createdDateTime'].map(lambda x: pd.to_datetime(x.split(' ')[0]))
		
		newTrans['value'] = newTrans['value'][-newTrans['value'].isnull()].map(lambda x: int(x))
		newTrans['amount'] = newTrans['amount'][-newTrans['amount'].isnull()].map(lambda x: int(x))

		return newTrans

	def getTransByComps(self, trans=DataFrame(), compIds=['315', '319', '305', '320'], reverse=False): 
		trans = self.trans if trans.empty else trans
		b = self.getBooleanByComps(trans, compIds)
		if reverse: return trans[-b]
		else: return trans[b]

	# get transaction of given product ids
	def getTransByPros(self, trans=DataFrame(), proids=['232', '231'], reverse=False):
		trans = self.trans if trans.empty else trans
		b = self.getBooleanByPros(trans, proids)
		if reverse: return trans[-b]
		else: return trans[b]

	def getTransByTeams(self, trans=DataFrame()): pass	

	# group daily trans number by weeks in a year
	# return list of trans count of different weeks
	def groupDayWeeklyTransCount(self, dayCount=Series()):
		dc = self.countDailyTrans() if dayCount.empty else dayCount
		return dc.groupby([dc.index.year, dc.index.week])

	# group daily trans number by month in a year
	def groupDayMonthlyTransCount(self, dayCount=Series()):
		dc = self.countDailyTrans() if dayCount.empty else dayCount
		return dc.groupby([dc.index.year, dc.index.month])			

	# group weekly trans number by month in a year
	# def groupWeekMonthTrans(): pass 

	# group trans number by each day of a week, using data of a month
	def groupDOWMonthlyTransCount(self, dayCount=Series()):
		dc = self.countDailyTrans() if dayCount.empty else dayCount
		return dc.groupby([dc.index.year, dc.index.month, dc.index.weekday])

	# group trans number by each day of a week, using data of a quarter	
	def groupDOWQuarterlyTransCount(): pass 

	# group trans number by each day of a week, using data of a month
	def groupDOWAnnuallyTransCount(self, dayCount=Series()):
		dc = self.countDailyTrans() if dayCount.empty else dayCount
		return dc.groupby([dc.index.year, dc.index.weekday])	

	def groupDayWeeklyTransRev(self, dayRev=Series()):
		rev = self.sumDailyRev() if dayRev.empty else dayRev
		return rev.groupby([rev.index.year, rev.index.week])		

	def groupDayMonthlyTransRev(self, dayRev=Series()):
		rev = self.sumDailyRev() if dayRev.empty else dayRev
		return rev.groupby([rev.index.year, rev.index.month])	

	def groupDOWMonthlyTransRev(self, dayRev=Series()):
		rev = self.sumDailyRev() if dayRev.empty else dayRev
		return rev.groupby([rev.index.year, rev.index.month, rev.index.weekday])		

	def groupDOWAnnuallyTransRev(self, dayRev=Series()):
		rev = self.sumDailyRev() if dayRev.empty else dayRev
		return rev.groupby([rev.index.year, rev.index.weekday])	

	# group by apply date	
	def groupTransByDates(self, df, dname='applyDate'):
		df[dname] = pd.to_datetime(df[dname])
		return df.groupby(df[dname])	
	
	def groupTransByTeams(self, df=DataFrame()):
		df = self.trans if df.empty else df
		return df.groupby(df['departmentId'])

	def groupTransByEmployees(self, df=DataFrame()): pass	

	# # def getMulTrans(self, dfs=''):
	# 	dfs = self.dfs if not dfs else dfs
	# 	return [self.getTrans(df) for df in dfs]

	# # the date of first transaction of the contract.
	# # apply to contract data only
	# def getFTranDates(self, df):
	# 	transgroup= df['transactions']
	# 	dates = [t[0]['applyDate'] for t in transgroup]
	# 	dates = pd.to_datetime(dates)
	# 	return dates		
	 
	# # dates of transactions. used for revenue exploration
	# # only use this with transaction DF
	# def getTranDates(self, df):
	# 	return df['applyDate']

	# # def getMulFTranDates(self, dfs=''):
	# 	dfs = self.dfs if not dfs else dfs
	# 	return [self.getConByComs(df) for df in self.dfs]

	# # def getMulTranDates(self, dfs=''):
	# 	dfs = self.dfs if not dfs else dfs
	# 	return [self.getTranDates(df) for df in self.dfs]

	# plot timeseries 
	def plotTimeSeriesTrans(self, df):
		df.plot()
		plt.show()

	def plotTimeSeriesDailyTrans(self, dayCount=Series()):
		c = self.countDailyTrans() if dayCount.empty else dayCount
		self.plotTimeSeriesTrans(c)

	def plotTimeSeriesWeeklyTrans(self, dayCount=Series()):
		c = self.countWeeklyTrans(dayCount)
		self.plotTimeSeriesTrans(c)

	def plotTimeSeriesMonthlyTrans(self, dayCount=Series()):
		c = self.countMonthlyTrans(dayCount)
		self.plotTimeSeriesTrans(c)	

	def plotTimeSeriesQuarterlyTrans(): pass

	def plotTimeSeriesAnnuallyTrans(): pass 

	# assume: df can be count, revenue, or something similar. Normally they are Series
	def plotStackedBar(self, df, kind='bar', legend=True):
		df.plot(kind=kind, stacked=True, alpha=1, legend=legend)
		plt.show()

	# in percentage
	def plotHbarDayMonthTrans(): pass 

	def plotHbarDayWeekTrans(): pass

	def plotHbarWeekMonthTrans(): pass
	
	# plot histogram of transaction counts each day of month
	def plotTransHist(): pass 

	# plot boxplot of multiple distributions like weeks of a month or months of ayear
	def plotBoxPlots(): pass

	def sumDailyRev(self, trans=DataFrame()):
		trans = self.trans if trans.empty else trans
		grouped = self.groupTransByDates(trans)
		rev = grouped['amount'].apply(lambda x: x.map(float).sum())		
		rev = rev.resample('D').sum()
		return rev[rev.index.weekday != 6] # remove Sunday

	def sumWeeklyRev(self, dayRev=Series()):
		rev = self.sumDailyRev() if dayRev.empty else dayRev
		return rev.resample('W', closed='right', kind='period').sum()			 

	def sumMonthlyRev(self, dayRev=Series()):
		rev = self.sumDailyRev() if dayRev.empty else dayRev
		return rev.resample('M', closed='right', kind='period').sum()		

	def sumQuarterlyRev(self, dayRev=Series()): pass

	def sumAnnuallyRev(self, dayRev=Series()): 
		rev = self.sumDailyRev() if dayRev.empty else dayRev
		return rev.resample('Y', closed='right', kind='period').sum()	

	# take groups of values, convert value into percentage, and store into a DF, whose column represents a group
	def toPctGroups(self, grouped):
		glist = []
		gnames = [i for i,g in grouped]
		gnames.sort()
		gnum = len(gnames)

		for n in range(gnum):
			agroup = grouped.get_group(gnames[n])
			pct = self.pct(agroup)
			glist.append(pct)

		df = self.concatx(glist).T
		df.index = gnames

		return df 

	###################### SPECIFIC #########################################
	def getDailyVerifiedTrans(self):
		return self.getTransByPros()

	def getDailyOtherTrans(self):
		return self.getTransByPros(reverse=True)	

	def getDayMonthlyVerifiedRev(self, trans=DataFrame()):
		trans = self.getDailyVerifiedTrans() if trans.empty else trans
		dRev = self.sumDailyRev(trans)
		revGrouped = self.groupDayMonthlyTransRev(dRev)
		return self.fromGroupsToDf(revGrouped)

	def getDayMonthlyOtherRev(self, trans=DataFrame()):
		trans = self.getDailyOtherTrans() if trans.empty else trans
		dRev = self.sumDailyRev(trans)
		revGrouped = self.groupDayMonthlyTransRev(dRev)
		return self.fromGroupsToDf(revGrouped)

# commission explorer
class CommExplorer(ConExplorer):
	def __init__(self, fnames='', tfile='json', dfs=''):
		ConExplorer.__init__(self, fnames, tfile, dfs)	

	# get successful commissions
	# df should be transactions returned by calling getTrans	
	def getComs(self, trans):
		df = trans[['commissions', 'id_tran', 'amount', 'value']]	
		transNum = len(df)

		# combine each transaction with corresponding staff id
		# calculate % commission if there are more than one employee
		# calculate correspondng commission
		commlist = []
		for n in range(transNum):
			tr = df.iloc[n]
			cs = tr['commissions']
			id_tran = tr['id_tran']
			amount = tr['amount']
			value = tr['value']
			[c.update({'id_tran': id_tran, 'actualComm': np.nan if not c['commission'] else (float(c['commission']) / value) * amount}) for c in cs]
			commlist.extend(cs)
		
		comms = DataFrame(commlist)
		comms = comms[-comms['actualComm'].isnull()]

		validTrans = trans.drop(['commissions'], axis=1)[-trans['id_tran'].isnull()]
		comms = pd.merge(comms, validTrans, left_on=['id_tran'], right_on=['id_tran'], suffixes=['', '_comm'])	
		return comms


# explorer of products
class ProExplorer(ConExplorer): 
	def __init__(self, fnames='', tfile='json', dfs=''):
		ConExplorer.__init__(self, fnames, tfile, dfs)

	# assume there is only one product in a contract. Not true for all product. but true with some important products	
	def getPros(self, df):
		pros = df['commissions']
		# get product names and product ids
		pid = df['commissions'].map(lambda x: x[0]['productId'] if x[0] else None)
		pname = df['commissions'].map(lambda x: x[0]['productName'] if x[0] else None)

		# STOP HERE. Not complete yet




