import psycopg2
import pandas as pd
conn = psycopg2.connect(host="localhost",database="postgres", user="postgres", password="itadmin")
conn1 = psycopg2.connect(host="10.100.55.163",database="postgres", user="postgres", password="itadmin")
conn2 = psycopg2.connect(host="10.100.53.173",database="postgres", user="postgres", password="itadmin")
conn3 = psycopg2.connect(host="10.100.53.63",database="postgres", user="postgres", password="itadmin")
conn4 = psycopg2.connect(host="10.100.52.203",database="postgres", user="postgres", password="itadmin")
conn5 = psycopg2.connect(host="10.100.53.228",database="postgres", user="postgres", password="itadmin")
conn6 = psycopg2.connect(host="10.100.54.217",database="postgres", user="postgres", password="itadmin")
conn7 = psycopg2.connect(host="10.100.54.205",database="postgres", user="postgres", password="itadmin")
conn8 = psycopg2.connect(host="10.100.53.233",database="postgres", user="postgres", password="itadmin")

# conn1 = psycopg2.connect(host="10.100.55.163",database="postgres", user="postgres", password="itadmin")
cur1 = conn1.cursor()
cur2 = conn2.cursor()
cur3 = conn3.cursor()
cur4 = conn4.cursor()
cur5 = conn5.cursor()
cur6 = conn6.cursor()
cur7 = conn7.cursor()
cur8 = conn8.cursor()
cur = conn.cursor()

cursor=[cur1,cur2,cur3,cur4,cur5,cur6,cur7,cur8]
# cur.execute('''CREATE TABLE COMPANY
#       (ID INT PRIMARY KEY     NOT NULL,
#       NAME           TEXT    NOT NULL,
#       AGE            INT     NOT NULL,
#       ADDRESS        CHAR(50),
#       SALARY         REAL);''')
def table_creation(cur):
	cur.execute('''CREATE TABLE Sales
	      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
	# cur.execute('''CREATE TABLE Sales1
	#       (	Order_ID INT PRIMARY KEY     NOT NULL);''')
	# cur.execute('''CREATE TABLE Sales2
	#       (	Order_ID INT PRIMARY KEY     NOT NULL);''')
	# cur.execute('''CREATE TABLE Sales3
	#       (	Order_ID INT PRIMARY KEY     NOT NULL);''')
	# cur.execute('''CREATE TABLE Sales4
	#       (	Order_ID INT PRIMARY KEY     NOT NULL);''')
	# cur.execute('''CREATE TABLE Sales5
	#       (	Order_ID INT PRIMARY KEY     NOT NULL);''')
	# cur.execute('''CREATE TABLE Sales6
	#       (	Order_ID INT PRIMARY KEY     NOT NULL);''')
	# cur.execute('''CREATE TABLE Sales7
	#       (	Order_ID INT PRIMARY KEY     NOT NULL);''')
	# cur.execute('''CREATE TABLE Sales8
	#       (	Order_ID INT PRIMARY KEY     NOT NULL);''')
	# cur.execute('''CREATE TABLE Sales9
	#       (	Order_ID INT PRIMARY KEY     NOT NULL);''')
	# cur.execute('''CREATE TABLE Sales10
	#       (	Order_ID INT PRIMARY KEY     NOT NULL);''')
	# cur.execute('''CREATE TABLE Sales11
	#       (	Order_ID INT PRIMARY KEY     NOT NULL);''')
	# cur.execute('''CREATE TABLE Sales12
	#       (	Order_ID INT PRIMARY KEY     NOT NULL);''')
	# cur.execute('''CREATE TABLE Sales13
	#       (	Order_ID INT PRIMARY KEY     NOT NULL);''')
	# cur.execute('''CREATE TABLE Sales14
	#       (	Order_ID INT PRIMARY KEY     NOT NULL);''')
	# cur.execute('''CREATE TABLE Sales15
	#       (	Order_ID INT PRIMARY KEY     NOT NULL);''')
	# cur.execute('''CREATE TABLE Sales16
	#       (	Order_ID INT PRIMARY KEY     NOT NULL);''')
	print('done')

# for i in cursor:
# 	table_creation(i)

conn1.commit()
conn2.commit()
conn3.commit()
conn4.commit()
conn5.commit()
conn6.commit()
conn7.commit()
conn8.commit()
# cur.execute('''truncate table Sales;''')
# conn.commit()
print("Table created")
def extract_data(url):
#     print('extract data...')
	data = pd.read_csv(url)
	d=data.values.tolist()
	header=list(data)
	dataset=[]
	for i in d:
	    x={}
	    for j in range(len(i)):
	        x.update({header[j]:i[j]})
	#         print(x)
	    dataset.append(x)
	#     for i in dataset:
	#         print(i)
	# random.shuffle(dataset)
	return dataset

url="1000000_Sales_Records.csv"
x=extract_data(url)
k=[]
c=0
for i in range(len(x)):
	print(i)
# 	# print(i,x[i]['Order ID'])
	if(x[i]['Order ID'] not in k):
		k.append(x[i]['Order ID'])
		cur.execute("INSERT INTO Sales(Order_ID) VALUES(%s) ",[x[i]['Order ID']])
		
	else:
		c=1
	print(x[i]['Order ID'])
	# cur.execute("INSERT INTO Sales(Order_ID) VALUES(%s) ",[x[i]['Order ID']])
# print c
#cur.executemany("INSERT INTO Sales(Order_ID) VALUES(%s) RETURNING vendor_id;);",)
print "Table created successfully"

conn.commit()
conn.close()

# Region,Country,Item Type,Sales Channel,Order Priority,Order Date,Order ID,Ship Date,Units Sold,Unit Price,Unit Cost,Total Revenue,Total Cost,Total Profit
