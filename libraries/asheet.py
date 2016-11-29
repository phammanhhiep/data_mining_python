def singleton(cls):
	instances = {}
	def wrapper(config):
		sheet_name = config['sheet_name']
		if not instances.get(sheet_name, ''):
			instances[sheet_name] = cls(config)
		else:
			pass # do nothing
		return instances[sheet_name]
	return wrapper


# represent a sheet
# sheet can be empty, filled with data, or not exist
@singleton
class ASheet:
	def __init__(self, config):
		self.spsh = config['spsh']
		self.sheet_name = config['sheet_name']		
		self.get_sheet()

	def get_sheet(self):
		default_row_num = 1
		default_row_num = 1
		
		try:
			self.sheet = self.spsh.worksheet(self.sheet_name)
		except Exception as ex:
			self.spsh.add_worksheet(self.sheet_name,default_row_num,default_row_num)
			self.sheet = self.spsh.worksheet(self.sheet_name)

	def add_row(self, name, num):
		sh = self.spsh.worksheet(name)
		sh.add_rows(num)

	def add_col(self,name,num):
		sh = self.spsh.worksheet(name)
		sh.add_cols(num)

	# only use after call get_sheet	
	def add_col_row(self, config):
		row_num = config.get('row_num', '')
		col_num = config.get('col_num', '')

		if row_num:
			self.sheet.add_rows(row_num)
		if col_num:
			self.sheet.add_cols(col_num)
	

	def del_row_col(self, config):
		pass	

	def get_no_data_first_row(self):
		return self.sheet.row_count + 1

	def get_value(self, rg):
		value = self.sheet.range(rg) # example of rg: "A1:B1"
		return value	
		
	def reset_sheet(self):
		pass

	def set_values(self, config):
		# get range
		label = config['label']
		vals = config['values']
		row_num = len(vals)
		col_num = len(vals[0])
		cell_index = 0
		rg = self.sheet.range(label)
		
		for row_index in range(row_num):
			for col_index in range(col_num):
				rg[cell_index].value = vals[row_index][col_index]
				cell_index += 1

		self.sheet.update_cells(rg)