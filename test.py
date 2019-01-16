import hashlib 
  
# initializing string 
str1 = "10"
  
# encoding GeeksforGeeks using encode() 
# then sending to md5() 
result = hashlib.md5(str1.encode()) 
  
# printing the equivalent hexadecimal value. 
print("The hexadecimal equivalent of hash is : ", end ="") 
print(result.hexdigest()) 
str1="15"

result = hashlib.md5(str1.encode()) 
  
# printing the equivalent hexadecimal value. 
print("The hexadecimal equivalent of hash is : ", end ="") 
print(result.hexdigest()) 

hash_list=[]

for i in range (0,128):
	str1=str(i)
	result = hashlib.md5(str1.encode())
	if result not in hash_list:
		hash_list.append(result)
print(len(hash_list))
print(hash_list)