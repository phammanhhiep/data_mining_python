import sys, os, math, requests
import datetime, logging, shelve
from collections import defaultdict

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


# represent a sql statement
# make it easier to reuse sql statement and add more complex query
class SQLObject:

	def __init__(self, config={}):
		config = defaultdict(str, config)
		self.select_stmt = ''
		self.from_stmt = ''
		self.where_stmt = ''

		self.add_select(config['select'])
		self.add_from(config['from'])
		self.add_where(config['where'])

		self.stmt_temp = '''
			SELECT {select}
			FROM {from}
			WHERE TRUE {AND} {where}

		'''

	def add_select(self, attrs):
		attr_str = ','.join(attrs)
		self.select_stmt = self.select_stmt + (',' if (self.select_stmt and attr_str) else '') + attr_str

	def add_where (self, conditions):
		for c in conditions:
			self.where_stmt = '{} {} {}'.format(self.where_stmt, ('AND' if self.where_stmt else ''), c)

	def add_from (self, table_join_stmts):
		for stmt in table_join_stmts:
			self.from_stmt = '{} {}'.format(self.from_stmt, stmt)

	# gen_stmt generate new statement, given the current select, from, and where clauses		
	def gen_stmt(self, sql_object=None):
		if sql_object:
			self.stmt = sql_object.gen_stmt()
		else:
			self.stmt = self.stmt_temp.format(**{
				'select': self.select_stmt, 
				'from': self.from_stmt,
				'where': self.where_stmt if self.where_stmt else '', 
				'AND': 'AND' if self.where_stmt else '',
			})
		return self.stmt 

	def reset_stmt(self):
		self.stmt = ''

class VGSQL(SQLObject):
	
	def __init__(self, config={}):
		SQLObject.__init__(self, config)





