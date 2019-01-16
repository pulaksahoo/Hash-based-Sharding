import psycopg2


conn1 = psycopg2.connect(host="127.0.0.2",database="postgres", user="postgres", password="itadmin")
conn2 = psycopg2.connect(host="127.0.0.3",database="postgres", user="postgres", password="itadmin")
conn3 = psycopg2.connect(host="127.0.0.4",database="postgres", user="postgres", password="itadmin")
conn4 = psycopg2.connect(host="127.0.0.5",database="postgres", user="postgres", password="itadmin")
conn5 = psycopg2.connect(host="127.0.0.6",database="postgres", user="postgres", password="itadmin")
conn6 = psycopg2.connect(host="127.0.0.7",database="postgres", user="postgres", password="itadmin")
conn7 = psycopg2.connect(host="127.0.0.8",database="postgres", user="postgres", password="itadmin")
conn8 = psycopg2.connect(host="127.0.0.9",database="postgres", user="postgres", password="itadmin")
cur1 = conn1.cursor()
cur2 = conn2.cursor()
cur3 = conn3.cursor()
cur4 = conn4.cursor()
cur5 = conn5.cursor()
cur6 = conn6.cursor()
cur7 = conn7.cursor()
cur8 = conn8.cursor()
cur1.execute('''CREATE TABLE Sales
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur2.execute('''CREATE TABLE Sales
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur3.execute('''CREATE TABLE Sales
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur4.execute('''CREATE TABLE Sales
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur5.execute('''CREATE TABLE Sales
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur6.execute('''CREATE TABLE Sales
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur7.execute('''CREATE TABLE Sales
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur8.execute('''CREATE TABLE Sales
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
print("Tables Created")
cur1.execute('''CREATE TABLE Sales1
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur1.execute('''CREATE TABLE Sales2
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur1.execute('''CREATE TABLE Sales3
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur1.execute('''CREATE TABLE Sales4
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur1.execute('''CREATE TABLE Sales5
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur1.execute('''CREATE TABLE Sales6
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur1.execute('''CREATE TABLE Sales7
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur1.execute('''CREATE TABLE Sales8
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
print("Tables Created")
cur2.execute('''CREATE TABLE Sales1
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur2.execute('''CREATE TABLE Sales2
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur2.execute('''CREATE TABLE Sales3
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur2.execute('''CREATE TABLE Sales4
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur2.execute('''CREATE TABLE Sales5
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur2.execute('''CREATE TABLE Sales6
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur2.execute('''CREATE TABLE Sales7
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur2.execute('''CREATE TABLE Sales8
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
print("Tables Created")
cur3.execute('''CREATE TABLE Sales1
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur3.execute('''CREATE TABLE Sales2
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur3.execute('''CREATE TABLE Sales3
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur3.execute('''CREATE TABLE Sales4
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur3.execute('''CREATE TABLE Sales5
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur3.execute('''CREATE TABLE Sales6
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur3.execute('''CREATE TABLE Sales7
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur3.execute('''CREATE TABLE Sales8
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
print("Tables Created")
cur4.execute('''CREATE TABLE Sales1
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur4.execute('''CREATE TABLE Sales2
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur4.execute('''CREATE TABLE Sales3
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur4.execute('''CREATE TABLE Sales4
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur4.execute('''CREATE TABLE Sales5
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur4.execute('''CREATE TABLE Sales6
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur4.execute('''CREATE TABLE Sales7
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur4.execute('''CREATE TABLE Sales8
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
print("Tables Created")
cur5.execute('''CREATE TABLE Sales1
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur5.execute('''CREATE TABLE Sales2
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur5.execute('''CREATE TABLE Sales3
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur5.execute('''CREATE TABLE Sales4
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur5.execute('''CREATE TABLE Sales5
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur5.execute('''CREATE TABLE Sales6
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur5.execute('''CREATE TABLE Sales7
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur5.execute('''CREATE TABLE Sales8
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
print("Tables Created")
cur6.execute('''CREATE TABLE Sales1
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur6.execute('''CREATE TABLE Sales2
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur6.execute('''CREATE TABLE Sales3
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur6.execute('''CREATE TABLE Sales4
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur6.execute('''CREATE TABLE Sales5
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur6.execute('''CREATE TABLE Sales6
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur6.execute('''CREATE TABLE Sales7
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur6.execute('''CREATE TABLE Sales8
	  (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur6.execute('''CREATE TABLE Sales1
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur6.execute('''CREATE TABLE Sales2
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur6.execute('''CREATE TABLE Sales3
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur6.execute('''CREATE TABLE Sales4
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur6.execute('''CREATE TABLE Sales5
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur6.execute('''CREATE TABLE Sales6
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur6.execute('''CREATE TABLE Sales7
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur6.execute('''CREATE TABLE Sales8
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
print("Tables Created")
cur7.execute('''CREATE TABLE Sales1
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur7.execute('''CREATE TABLE Sales2
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur7.execute('''CREATE TABLE Sales3
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur7.execute('''CREATE TABLE Sales4
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur7.execute('''CREATE TABLE Sales5
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur7.execute('''CREATE TABLE Sales6
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur7.execute('''CREATE TABLE Sales7
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur7.execute('''CREATE TABLE Sales8
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
print("Tables Created")
cur8.execute('''CREATE TABLE Sales1
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur8.execute('''CREATE TABLE Sales2
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur8.execute('''CREATE TABLE Sales3
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur8.execute('''CREATE TABLE Sales4
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur8.execute('''CREATE TABLE Sales5
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur8.execute('''CREATE TABLE Sales6
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur8.execute('''CREATE TABLE Sales7
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
cur8.execute('''CREATE TABLE Sales8
      (	Order_ID INT PRIMARY KEY     NOT NULL);''')
print("Tables Created")
conn1.commit()
conn2.commit()
conn3.commit()
conn4.commit()
conn5.commit()
conn6.commit()
conn7.commit()
conn8.commit()


cur1.execute('''truncate table Sales;''')
cur1.execute('''truncate table Sales1;''')
cur1.execute('''truncate table Sales2;''')
cur1.execute('''truncate table Sales3;''')
cur1.execute('''truncate table Sales4;''')
cur1.execute('''truncate table Sales5;''')
cur1.execute('''truncate table Sales6;''')
cur1.execute('''truncate table Sales7;''')
cur1.execute('''truncate table Sales8;''')
print("Table Truncated")

cur2.execute('''truncate table Sales;''')
cur2.execute('''truncate table Sales1;''')
cur2.execute('''truncate table Sales2;''')
cur2.execute('''truncate table Sales3;''')
cur2.execute('''truncate table Sales4;''')
cur2.execute('''truncate table Sales5;''')
cur2.execute('''truncate table Sales6;''')
cur2.execute('''truncate table Sales7;''')
cur2.execute('''truncate table Sales8;''')
print("Table Truncated")

cur3.execute('''truncate table Sales;''')
cur3.execute('''truncate table Sales1;''')
cur3.execute('''truncate table Sales2;''')
cur3.execute('''truncate table Sales3;''')
cur3.execute('''truncate table Sales4;''')
cur3.execute('''truncate table Sales5;''')
cur3.execute('''truncate table Sales6;''')
cur3.execute('''truncate table Sales7;''')
cur3.execute('''truncate table Sales8;''')
print("Table Truncated")

cur4.execute('''truncate table Sales;''')
cur4.execute('''truncate table Sales1;''')
cur4.execute('''truncate table Sales2;''')
cur4.execute('''truncate table Sales3;''')
cur4.execute('''truncate table Sales4;''')
cur4.execute('''truncate table Sales5;''')
cur4.execute('''truncate table Sales6;''')
cur4.execute('''truncate table Sales7;''')
cur4.execute('''truncate table Sales8;''')
print("Table Truncated")

cur5.execute('''truncate table Sales;''')
cur5.execute('''truncate table Sales1;''')
cur5.execute('''truncate table Sales2;''')
cur5.execute('''truncate table Sales3;''')
cur5.execute('''truncate table Sales4;''')
cur5.execute('''truncate table Sales5;''')
cur5.execute('''truncate table Sales6;''')
cur5.execute('''truncate table Sales7;''')
cur5.execute('''truncate table Sales8;''')
print("Table Truncated")

cur6.execute('''truncate table Sales;''')
cur6.execute('''truncate table Sales1;''')
cur6.execute('''truncate table Sales2;''')
cur6.execute('''truncate table Sales3;''')
cur6.execute('''truncate table Sales4;''')
cur6.execute('''truncate table Sales5;''')
cur6.execute('''truncate table Sales6;''')
cur6.execute('''truncate table Sales7;''')
cur6.execute('''truncate table Sales8;''')
print("Table Truncated")

cur7.execute('''truncate table Sales;''')
cur7.execute('''truncate table Sales1;''')
cur7.execute('''truncate table Sales2;''')
cur7.execute('''truncate table Sales3;''')
cur7.execute('''truncate table Sales4;''')
cur7.execute('''truncate table Sales5;''')
cur7.execute('''truncate table Sales6;''')
cur7.execute('''truncate table Sales7;''')
cur7.execute('''truncate table Sales8;''')
print("Table Truncated")

cur8.execute('''truncate table Sales;''')
cur8.execute('''truncate table Sales1;''')
cur8.execute('''truncate table Sales2;''')
cur8.execute('''truncate table Sales3;''')
cur8.execute('''truncate table Sales4;''')
cur8.execute('''truncate table Sales5;''')
cur8.execute('''truncate table Sales6;''')
cur8.execute('''truncate table Sales7;''')
cur8.execute('''truncate table Sales8;''')

print("Table Truncated")


conn1.commit()
conn2.commit()
conn3.commit()
conn4.commit()
conn5.commit()
conn6.commit()
conn7.commit()
conn8.commit()
print("Table committed")


conn1.close()
conn2.close()
conn3.close()
conn4.close()
conn5.close()
conn6.close()
conn7.close()
print("connection closed")
conn8.close()
