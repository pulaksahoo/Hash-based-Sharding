import psycopg2
import hashlib
conn = psycopg2.connect(host="localhost",database="postgres", user="postgres", password="itadmin")
# conn1 = psycopg2.connect(host="127.0.0.2",database="postgres", user="postgres", password="itadmin")
# conn2 = psycopg2.connect(host="127.0.0.3",database="postgres", user="postgres", password="itadmin")
# conn3 = psycopg2.connect(host="127.0.0.4",database="postgres", user="postgres", password="itadmin")
# conn4 = psycopg2.connect(host="127.0.0.5",database="postgres", user="postgres", password="itadmin")
# conn5 = psycopg2.connect(host="127.0.0.6",database="postgres", user="postgres", password="itadmin")
# conn6 = psycopg2.connect(host="127.0.0.7",database="postgres", user="postgres", password="itadmin")
# conn7 = psycopg2.connect(host="127.0.0.8",database="postgres", user="postgres", password="itadmin")
# conn8 = psycopg2.connect(host="127.0.0.9",database="postgres", user="postgres", password="itadmin")
conn1 = psycopg2.connect(host="10.100.52.206",database="postgres", user="postgres", password="itadmin")   #Shreya
conn2 = psycopg2.connect(host="10.100.53.173",database="postgres", user="postgres", password="itadmin")   #raghu
conn3 = psycopg2.connect(host="10.100.52.250",database="postgres", user="postgres", password="itadmin")    #tushar
conn4 = psycopg2.connect(host="10.100.53.30",database="postgres", user="postgres", password="itadmin")   #kaif
conn5 = psycopg2.connect(host="10.100.52.233",database="postgres", user="postgres", password="itadmin")   #Abhishek
conn6 = psycopg2.connect(host="10.100.54.217",database="postgres", user="postgres", password="itadmin")   #suraj
conn7 = psycopg2.connect(host="10.100.54.22",database="postgres", user="postgres", password="itadmin")   #gautam
conn8 = psycopg2.connect(host="10.100.53.233",database="postgres", user="postgres", password="itadmin")   #pulak
cur = conn.cursor()
# cur1 = conn.cursor()
# cur2 = conn.cursor()
# cur3 = conn.cursor()
# cur4 = conn.cursor()
# cur5 = conn.cursor()
# cur6 = conn.cursor()
# cur7 = conn.cursor()
# cur8 = conn.cursor()
cur1 = conn1.cursor()
cur2 = conn2.cursor()
cur3 = conn3.cursor()
cur4 = conn4.cursor()
cur5 = conn5.cursor()
cur6 = conn6.cursor()
cur7 = conn7.cursor()
cur8 = conn8.cursor()


# key_space=2**128
# hash_list=[]
# i=0
# while (i<=2048):
# 	str1=str(i)
# 	print( hashlib.md5(str1.encode()).hexdigest())
# 	result=hex(i)
# 	if result not in hash_list:
# 		hash_list.append(result[2:].zfill(128//4))
# 	i+=1
# number_of_shards=128;
# shards_per_server=number_of_shards//8;
threshold=[5700,5700,5700,5700,5700,5700,5700,5700]
nv_shard_sub_tables=['1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16']
server_list=['1','2','3','4','5','6','7','8']
sub_tables=['sales1','sales2','sales3','sales4','sales5','sales6','sales7','sales8']

# print(hash_list[:5])
# cur.execute('''select order_id from sales;''')
# print(cur.rowcount);
# rows = cur.fetchall()
# i=1
# key_space=[]
# for row in rows:
# 	# print(i,row[0])
# 	str1=str(row[0])
# 	if(hashlib.md5(str1.encode()).hexdigest() not in key_space):
# 		key_space.append( hashlib.md5(str1.encode()).hexdigest())
# 	i+=1
# 	# row = cur.fetchone()
# print(len(key_space))
# if(hashlib.md5('1'.encode()).hexdigest() in key_space):
# 	print(True)
# else:
# 	print(False)
key_space_size=2**128
def create_shard(S,V,key_space_size):
	virtual_partition_size=key_space_size//V
	v=V//S
	cnt=0
	LB=[]
	for i in range(S):
		x=[]
		for i in range(v):
			x.append(0)
		LB.append(x) 
	for i in range(S):
		for j in range(v):
			LB[i][j]=cnt*virtual_partition_size
			cnt+=1
	return LB
	pass
# print(create_shard(8,2**7,key_space_size))
def new_create_shard(S,T,key_space_size):
	T_sum=0
	for i in T:
		T_sum+=i
	# print(T[0]/T_sum)
	# print('HI',T_sum,key_space_size)
	LB=[]
	for i in range(S):
		key_space_ratio=T[i]/T_sum*100
		# print(T[i]/T_sum*100)
		shard_size=key_space_ratio/100*key_space_size
		# print(shard_size)
		if(i==0):
			LB.append(shard_size)
		else:
			LB.append(LB[i-1]+shard_size)
	return LB

	pass
# print('ks',key_space_size)

# print('new: ',new_create_shard(8,threshold,key_space_size))
cursor=[cur1,cur2,cur3,cur4,cur5,cur6,cur7,cur8]

def insert_old_shard(LB):
	cur.execute('''select order_id from sales;''')
	print(cur.rowcount);
	rows = cur.fetchall()
	i=1
	key_space=[]
	count=[0,0,0,0,0,0,0,0]
	x1=1
	for row in rows:
		# print(i,row[0])
		str1=str(row[0])
		hash_string=hashlib.md5(str1.encode()).hexdigest()
		for i in range(len(LB)):
			for j in range(len(LB[i])):
				# print(i)
				if(j>=8):
					x=2*(i+1)
				else:
					x=(2*i)+1
				if(j!=len(LB[i])-1):
					if(int(hash_string,16)>=LB[i][j] and int(hash_string,16)<LB[i][j+1]):
						# print(i,j)
						count[j%8]+=1
						
						# query="INSERT INTO "+'sales'+str((j%8)+1)+str(x)+"(Order_ID) VALUES(%s) "
						query="INSERT INTO "+'sales'+str(x)+"(Order_ID) VALUES(%s) "
						# cursor[j%8].execute(query,[int(row[0])])
				else:
					# print(i,j)
					if(i!=7):
						if(int(hash_string,16)>=LB[i][j] and int(hash_string,16)<LB[i+1][0] ):
							# print(i,j)
							count[j%8]+=1
							# query="INSERT INTO "+'sales'+str((j%8)+1)+str(x)+"(Order_ID) VALUES(%s) "
							query="INSERT INTO "+'sales'+str(x)+"(Order_ID) VALUES(%s) "
							# cursor[j%8].execute(query,[int(row[0])])

					else:
						if(int(hash_string,16)>=LB[i][j] and int(hash_string,16)<=key_space_size and i!=8):
							count[j%8]+=1
							# query="INSERT INTO "+'sales'+str((j%8)+1)+str(x)+"(Order_ID) VALUES(%s) "
							query="INSERT INTO "+'sales'+str(x)+"(Order_ID) VALUES(%s) "
							# cursor[j%8].execute(query,[int(row[0])])
		print(x1)
		x1+=1

		# if(hashlib.md5(str1.encode()).hexdigest() not in key_space):
		# 	key_space.append( hashlib.md5(str1.encode()).hexdigest())
		# i+=1
		# row = cur.fetchone()order_id from sales;''')
	# rows = cur.fetchall()
	print(count)
	return count
	pass

# count1=insert_old_shard(create_shard(8,2**7,key_space_size))
# connection=[conn1,conn2,conn3,conn4,conn5,conn6,conn7]

def insert_new_shard(LB):
	cur.execute('''select order_id from sales;''')
	# print(cur.rowcount);
	rows = cur.fetchall()
	print(LB)
	count=[0,0,0,0,0,0,0,0]
	x=1
	print(LB)
	for row in rows:
		# print(i,row[0])
		str1=str(row[0])
		hash_string=hashlib.md5(str1.encode()).hexdigest()
		for i in range(len(LB)):
			if(i==0):
				if(int(hash_string,16)>=0 and int(hash_string,16)<=LB[i]):
					#query="INSERT INTO "+sub_tables[i]+"(Order_ID) VALUES(%s) "
					query="INSERT INTO "+'sales'+"(Order_ID) VALUES(%s) "
					# cursor[i].execute(query,[int(row[0])])
					count[i]+=1
			else:
				
				print(i)
				if(int(hash_string,16)>LB[i-1] and int(hash_string,16)<=LB[i]):
					print(i)
					# connection[i].execute("INSERT INTO Sales(Order_ID) VALUES(%s) ",[int(row[0])])
					count[i]+=1
					# query="INSERT INTO "+sub_tables[i]+"(Order_ID) VALUES(%s) "
					query="INSERT INTO "+'sales'+"(Order_ID) VALUES(%s) "
					# cursor[i].execute(query,[int(row[0])])

		# print(x,count)
		x+=1
	return count
	return 1
	pass

count2=insert_new_shard(new_create_shard(8,threshold,key_space_size))
conn.commit()
conn1.commit()
conn2.commit()
conn3.commit()
conn4.commit()
conn5.commit()
conn6.commit()
conn7.commit()
conn8.commit()
conn.close()
conn1.close()
conn2.close()
conn3.close()
conn4.close()
conn5.close()
conn6.close()
conn7.close()
conn8.close()


# print("count1: ",count1)
print("count2: ",count2)


def shard_split():
	pass