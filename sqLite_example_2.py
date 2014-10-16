import sqlite3



c = sqlite3.connect('premium.db')
curs = c.cursor()

curs.execute( ''' create table due_to_renew
			  (reporting_year integer,
			  	reporting_week integer,
			  	is_auto_renew_enabled text,
			  	customer_id text)''')
c.commit()
list_of_rows = []
with open('C:/Users/delandre/Desktop/renew_due_wk18.tsv', 'r') as file_object:
	for line in file_object:
		row = line.split('\t')
		list_of_rows.append(tuple(row))
curs.executemany('''insert into due_to_renew (reporting_year, reporting_week, is_auto_renew_enabled, customer_id ) values (?, ?, ?, ?)''', list_of_rows)
c.commit()

def create_table(dbname, table_name, list_of_columns):
	""" Creates and table on the database provided or it will create
		a new database if the provided one doesn't exist.
		list_of_columns is a list of tuples containing the name of the column
		the data type (column_name, data_type)
	"""

	c = sqlite3.connect(dbname)
	colums = [ c_name +' ' +data_type for c_name, data_type in list_of_columns ]
	create_columns_str = ', '.join(colums)
	curs = c.cursor()
	curs.execute( ''' create table {0} ({1})'''.format(table_name, create_columns_str))




