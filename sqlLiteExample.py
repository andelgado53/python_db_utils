import sqlite3


def format_date(thedate):
  months={'JAN':'01','FEB':'02','MAR':'03','APR':'04', 
  'MAY': '05','JUN': '06', 'JUL' : '07', 'AUG': 
  '08', 'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12'}
	years = {'09':'2009', '10':'2010', '11':'2011', '12':'2012', '13':'2013', '14':'2014'}
	formated_date = ''
	if thedate:
		thedate = thedate.split('-')
		formated_date = formated_date + years[thedate[2]] +'-'+months[thedate[1]]+'-'+thedate[0]
		return formated_date
	else:
		return formated_date

c = sqlite3.connect('kindle.db') # connects to a database stored in the same directory as the python file, or it creates a database
curs = c.cursor() # creates a cursor to execute commands
curs.execute('''create table devices 
             (first_radio_on text,
              fro_week text,
              fro_day text,
              first_time_registered_utc text,
              ftr_week text,
              ftr_day text,
              device_type text,
              gen text,
              device text,
              fiona_serial_number text,
              customer_id text,
              first_access_date text,
              first_active_date text
            
             ) ''') # this creates a table the database
c.commit()
cnt = 0
ls = []
for line in open('C:/Users/delandre/Documents/Documents/Projects/top_quint_freq/dev.txt'): # this reads the data from a text file and formats it according to the database schema
   if 'first_radio_on' in line:
     continue
   elif cnt < 1000000 :
     #row = line.replace('\t', ',')
     row = line.split('\t')
     row[0] = format_date(row[0])
     row[1] = format_date(row[1])
     row[2] = format_date(row[2])
     row[3] = format_date(row[3])
     row[4] = format_date(row[4])
     row[5] = format_date(row[5])
     #row[11] = format_date(row[11])
     #row[12] = format_date(row[12])      
     ls.append(tuple(row))
     cnt = cnt + 1
   else:
     curs.executemany(''' insert into devices(first_radio_on, fro_week, fro_day, first_time_registered_utc, ftr_week,ftr_day, device_type, gen, device, fiona_serial_number, customer_id, first_access_date, first_active_date) values( ? , ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', ls) # this inserts the data into the new table in a single batch 
     cnt = 0
     ls = []
curs.executemany(''' insert into devices(first_radio_on, fro_week, fro_day, first_time_registered_utc, ftr_week,ftr_day, device_type, gen, device, fiona_serial_number, customer_id, first_access_date, first_active_date) values( ? , ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', ls) # this inserts the data into the new table in a single batch 

d = curs.execute('select * from devices where customer_id = "1181748990" ')
  
for l in d:
  for item in l:
    print(item)


    
    

c.commit() # commits changes
c.close()

