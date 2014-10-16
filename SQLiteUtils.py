import sqlite3
import sys

def create_table(dbname, table_name, list_of_columns):
	""" Creates and table on the database provided or it will create
		a new database if the provided one doesn't exist.
		list_of_columns is a list of tuples containing the name of the column
		the data type (column_name, data_type). If table already exist an error will
		be raised
	"""

	c = sqlite3.connect(dbname)
	colums = [ c_name +' ' +data_type for c_name, data_type in list_of_columns ]
	create_columns_str = ', '.join(colums)
	curs = c.cursor()
	curs.execute( ''' create table {0} ({1})'''.format(table_name, create_columns_str))
	c.commit()
	c.close()
	file_path = sys.argv[0].split('\\')
	file_path = '/'.join(file_path[0:-1] + [''])
	finish_message = 'New table is here: ' + file_path + dbname
	print( finish_message)




def insert_data(file_path, dbname, table_name, list_of_columns, sep = '\t', new_table = False):
	
	if new_table:
		create_table(dbname, table_name, list_of_columns)
		
	c = sqlite3.connect(dbname)
	curs = c.cursor()
	list_of_rows = []
	with open(file_path, 'r') as file_object:
		for line in file_object:
			row = line.strip().split(sep)
			list_of_rows.append(tuple(row))
	colums = [ c_name for c_name, data_type in list_of_columns ]
	create_columns_str = ', '.join(colums)
	values = '?,' * len(colums)
	values = values[0:-1]
	curs.executemany('''insert into {0}({1}) values ({2})'''.format(table_name,create_columns_str, values) , list_of_rows )
	c.commit()
	c.close()


def display_results(query_result, limit = None):
	'''Helper function that returns a formatted version of a SQLITE query result object'''

	if limit:

		row = 0
		for result in query_result:
			if row <  limit:
				print '\t'.join(map(str,result)).strip()
				row = row + 1
			else:
				break
	else:
		for result in query_result:
			print '\t'.join(map(str,result)).strip()
			



def select_top_n(dbname, table_name, n, order_by = None):
	'''Returns the top n rows on the table porvided. Results can be ordered by using the 
		order_by variable in the function
	'''

	c = sqlite3.connect(dbname)
	curs = c.cursor()
	if order_by:
		query = curs.execute('select * from {0} order by {1} '.format(table_name, order_by))
	else:
		query = curs.execute('select * from {0} '.format(table_name))
	display_results(query, n)
	c.close()

def run_query(dbname, query):
	''' Displays the results of any query provided '''

	c = sqlite3.connect(dbname)
	curs = c.cursor()
	query = curs.execute(query)
	display_results(query)
	c.close()


#create_table('test.db', 'test_table', [('column_1', 'text'), ('column_2', 'text')])
#insert_data('test.txt', 'test_1.db', 'test_table', [('column_1', 'text'), ('column_2', 'text')])
#select_top_n('premium.db', 'wk18_due',  10, order_by = 'customer_id')

q = 'select count(distinct customer_id), auto_renew from wk18_due group by auto_renew'
		
run_query( 'premium.db',q)
#c = [('year', 'text'), ('week', 'text'), ('auto_renew', 'text'), ('customer_id', 'text') ]
#create_table('premium.db', 'wk18_due', c) 
#insert_data('C:/Users/delandre/Desktop/renew_due_wk18.tsv', 'premium.db', 'wk18_due', c)
