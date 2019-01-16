# from dbms_assignment2 import create_shard
import psycopg2
import hashlib
conn1 = psycopg2.connect(host="10.100.52.206",database="postgres", user="postgres", password="itadmin")   #Shreya
conn2 = psycopg2.connect(host="10.100.53.173",database="postgres", user="postgres", password="itadmin")   #raghu
conn3 = psycopg2.connect(host="10.100.52.250",database="postgres", user="postgres", password="itadmin")    #tushar
conn4 = psycopg2.connect(host="10.100.53.30",database="postgres", user="postgres", password="itadmin")   #kaif
conn5 = psycopg2.connect(host="10.100.52.233",database="postgres", user="postgres", password="itadmin")   #Abhishek
conn6 = psycopg2.connect(host="10.100.54.217",database="postgres", user="postgres", password="itadmin")   #suraj
# conn7 = psycopg2.connect(host="10.100.54.22",database="postgres", user="postgres", password="itadmin")   #gautam
conn8 = psycopg2.connect(host="10.100.53.233",database="postgres", user="postgres", password="itadmin")   #pulak
cur1 = conn1.cursor()
cur2 = conn2.cursor()
cur3 = conn3.cursor()
cur4 = conn4.cursor()
cur5 = conn5.cursor()
cur6 = conn6.cursor()
# cur7 = conn7.cursor()
cur8 = conn8.cursor()


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
threshold=[5700,5700,5700,5700,5700,5700,5700,5700]
count1= [112064, 112972, 113271, 112926, 112464, 111576, 112404, 112323]
count2=  [112771, 112321, 112487, 112240, 112916, 112049, 112582, 112634]
# cursor=[cur1,cur2,cur3,cur4,cur5,cur6,cur7,cur8]
cursor=[cur1,cur2,cur3,cur4,cur5,cur6,cur8]
split_value1=[]
split_value2=[]
remaining1=[]
remaining2=[]

def shard_split():
	split_percentage1=[]
	split_percentage2=[]
	for i in range (8):
		print(i)
		print(cursor[i].execute('Select count(*) from (select * from sales1 union all select * from sales2 union all select * from sales3 union all select * from sales4 union all select * from sales5) as f'))
		rows=cursor[i].fetchall()[0][0]
		split_percentage1.append(rows/count1[i]*100)
		
		split_value1.append(int(rows))
		remaining1.append(count1[i]-rows)
	# LB=create_shard(8,2**7,2**128)
	# lower=LB[2][2]
	# upper=LB[2][3]
	# key_value=80/100*(upper-lower)
	# upper=lower+key_value
	# cur7.execute('Select * from sales5')
	# rows = cur7.fetchall()
	# count=0
	# for i in rows:
	# 	# print(i[0])
	# 	hash_string=hashlib.md5(str(i[0]).encode()).hexdigest()
	# 	hash_string_int=int(hash_string,16)
	# 	# print(hash_string_int)
	# 	# print(lower,'hi')
	# 	# print(upper,'hello')
	# 	if(hash_string_int>=lower and hash_string_int<=upper):
	# 		count+=1
	# print('count: ',count)
		print(i)
		cursor[i].execute('select * from sales')
		LB=new_create_shard(8,threshold,2**128)
		if(i==0):
			lower=0
		else:
			lower=LB[i-1]
		upper=LB[i]
		upper=lower+(0.3*(upper-lower))
		rows=cursor[i].fetchall()
		count=0
		for j in rows:
			hash_string=hashlib.md5(str(j[0]).encode()).hexdigest()
			hash_string_int=int(hash_string,16)
			if(hash_string_int>=lower and hash_string_int<upper):
				count+=1
		pass
		split_percentage2.append(count/count2[i]*100)
		split_value2.append(count)
		remaining2.append(count2[i]-count)
	print('split1 %: ',split_percentage1)
	print('split2 %: ',split_percentage2)
	print('split value1: ',split_value1)
	print('split value2: ',split_value2)
	print('remaining1: ',remaining1)
	print('remaining2: ',remaining2)

shard_split()


# 0.300705264

# 0.300078289