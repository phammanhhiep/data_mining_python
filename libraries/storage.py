import pymysql
import requests
from collections import defaultdict
from abc import ABCMeta, abstractmethod
import threading, time, logging, datetime, json, csv, shelve
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google2pandas import *

#############################################

from asheet import ASheet
from common_lib import singleton
import common_lib
import vgvars

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class Storage(metaclass=ABCMeta):
	@abstractmethod
	def connect(self): pass
	@abstractmethod
	def query(self): pass
	@abstractmethod
	def fetch(self): pass
	@abstractmethod
	def insert(self): pass
	@abstractmethod
	def delete(self): pass

class APIStorage(Storage):
	def __init__(self, config):
		self.domain = config['domain']

	def connect(self): 
		pass
	def query(self, stmt):
		r = requests.get(stmt)
		return r.json()

	def fetch(self, stmt): 
		data = self.query('{}{}'.format(self.domain, stmt))
		return data

	def insert(self): pass
	def delete(self): pass	

# represent a database
class DBStorage(Storage):
	def __init__(self, config): 
		self.config = config
		self.data = '' # placeholder

	def connect(self):
		cnx = pymysql.connect(**self.config)
		self.conn = cnx
		self.cur = self.conn.cursor()

	def query(self, stmt):
		self.cur.execute(stmt)

	# try 3 times if disconnect, and if fail again, then exit	
	def fetch(self, stmt):
		try:
			self.connect()
			self.query(stmt)
			data = self.cur.fetchall()
		except Exception as ex:
			print(ex)
			print('try again first time ...')
			try:
				self.connect()
				self.query(stmt)
				data = self.cur.fetchall()		
			except Exception as ex:
				print(ex)
				print('try again second time ...')
				try:
					self.connect()
					self.query(stmt)
					data = self.cur.fetchall()
				except Exception as ex:
					print(ex)
					print('try again third (last) time ...')
					try:
						self.connect()
						self.query(stmt)
						data = self.cur.fetchall()	
					except Exception as ex:
						print(ex)
						print('failed after 3 times. exit now')	
						self.close()
						exit()		

		else:	
			self.close()
		return data

	def close(self):
		self.conn.close()
		self.cur.close()

	def insert(self): pass
	def delete(self): pass


# represent a google analytics account
class GAStorage(Storage):
	def __init__(self):
		self.connect()		

	def connect(self):
		conn = GoogleAnalyticsQuery(secrets=vgvars.google_ga_client_secrets_path, token_file_name=vgvars.dir_path['config'] + 'analytics.dat')

	def query(self): pass

	def fetch(self): pass

	def insert(self): pass

	def delete(self): pass


# represent a google spreadsheet
class GSStorage(Storage):
	def __init__(self, config):
		self.config = config
		self.app_key = config['google_app_key_file']
		self.file_id = config['file_id']
		self.connected = ''
		self.authorized = ''
		self.aSheets = {} 
		self.data = '' # placeholder

	def authorize(self, scope=['https://spreadsheets.google.com/feeds']):
		app_key = self.app_key
		credentials = ServiceAccountCredentials.from_json_keyfile_name(app_key, scope)
		google_credentials = gspread.authorize(credentials)
		self.authorized = True
		return google_credentials

	def add_sheet(self,name, row_num, col_num):
		self.spsh.add_worksheet(name, row_num, col_num)

	def connect (self):
		if not self.connected:
			if not self.authorized:
				gc = self.authorize()
			self.spsh = gc.open_by_key(self.file_id)
			self.sheets = self.spsh.worksheets()
			self.sheet_num = len(self.sheets)
			self.connected = True

	# def create_asheet(self, config):
	def create_asheet(self, sheet_name):
		self.aSheets[sheet_name] = ASheet({
			'sheet_name':sheet_name,
			'spsh': self.spsh
		})

		return self.aSheets[sheet_name]

	def delete_sheet(self,sheet_name):
		try:
			sheet = self.spsh.worksheet(sheet_name)
			self.spsh.del_worksheet(sheet)
		except Exception as ex:
			logging.debug(ex)

	def delete(self): pass		

	def fetch(self, config): 
		return self.query(config)

	# examine the function	
	def insert(self, config):
		self.connect()
		sheet_name = config['sheet_name']
		label = config['label']
		vals = config['values']
		sh = self.create_asheet(sheet_name)
		sh.set_values({'values': vals, 'label': label})			

	def is_new_file(self):
		default_sheet_name = 'Sheet1'
		try:
			self.spsh.worksheet(default_sheet_name)
		except Exception as ex:
			print(ex)
			self.file_init = False
			return False
		self.file_init = True
		return True

	def query(self, config): 
		if self.app_key and self.file_id:
			self.connect()	

		sheet_name = config['sheet_name']	
		rg = config['range']

		sh = self.create_asheet(sheet_name)
		return sh.get_value(rg)

	def reset_file(self, sheetnames):
		# remove all sheetnames specified
		# add a new sheet to mimic a untouch sheet
		# count number of sheets in the raw
		
		sname_num = len(sheetnames)

		if self.sheet_num <= sname_num:
			self.add_sheet('Sheet1',1,1)
			self.file_init = True

		for sname in sheet_names:
			self.del_sheet(sname)	


class FileStorage(Storage):
	def __init__(self, config):
		self.fpath = config['fpath']
		self.data = '' # placeholder

	def connect(self, fpath='', selected_format='csv', mode='r'):
		if not fpath:
			fpath = self.fpath

		if selected_format == 'csv':
			fd = open(fpath, 'r', newline='', encoding='utf-8')
			self.fd = csv.reader(fd, delimiter=',')

		elif selected_format == 'shelve':
			self.fd = shelve.open(fpath)

		elif selected_format == 'json':
			pass 

	def query(self): pass

	def fetch (self, config={}, fpath='', selected_format='csv', mode='r'):
		if config.get('fpath', ''):
			fpath = config['fpath']
		if config.get('selected_format', ''):
			selected_format = config['selected_format']	

		self.connect(fpath=fpath, selected_format=selected_format, mode=mode)
		
		return self.fd

	# append data by default
	# data is a list of list
	def insert(self, config, fpath='', selected_format='csv', mode='a'): 
		config = defaultdict(str, config)

		if not config.get('values', ''):
			logging.debug('No values provided')
			return
		else:
			data = config['values']

		if config['selected_format']:
			selected_format = config['selected_format']	

		if not fpath:
			fpath = self.fpath

		if selected_format == 'csv':
			common_lib.write_csv(fpath, data, mode)
		elif selected_format == 'text':
			pass
		# 
		elif selected_format == 'shelve':
			with shelve.open(fpath) as sh:
				key_index = config['key_index']
				for r in data:
					k = r[key_index]
					sh[str(k)] = r
		elif selected_format == 'json': 
			# datajson = json.dumps(data, indent=4, ensure_ascii=False)
			datajson = json.dumps(data, ensure_ascii=False)
			common_lib.write_text(fpath, datajson)

	def delete(self): pass	


# need such class to reuse the interface
# it is no more than a variable
class VarStorage(Storage):
	def __init__(self, config=''):
		self.data = [] if not config else config['data']

	def connect(self): pass
	def query(self): pass
	def fetch(self): 
		return self.data

	# data is a list of list or similar data structure	
	def insert(self, config):
		try:
			test = self.data
		except AttributeError as ex:
			self.data = []	
		finally:
			# logging.debug('data length: {}'.format(len(self.data)))	
			self.data.extend(config['values'])

	def delete(self):
		del self.data