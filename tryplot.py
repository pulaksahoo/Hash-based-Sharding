# count1= [112064, 112972, 113271, 112926, 112464, 111576, 112404, 112323]
# count2=  [112771, 112321, 112487, 112240, 112916, 112049, 112582, 112634]
count1=[31.088485151342088, 31.2581878695606, 31.2162866046914, 31.333793811876802, 31.368260065443167, 31.470925647092564, 31.275577381587844, 31.242933326211013]
count2=[30.287928634134666, 30.02288085041978, 30.164374549947993, 30.106022808267994, 30.257890821495625, 29.759301734062777, 30.070526371888935, 30.16407123959018]

import matplotlib.pyplot as plt
x = [1,2,3,4,5,6,7,8]
y = count1
y1 = count2
plt.xlabel("X-axis")
plt.ylabel("Y-axis")
plt.title("A test graph")
# for i in range(len(y)):
plt.plot(x,[pt for pt in y],label='old model' )
plt.plot(x,[pt for pt in y1],label='new model')
plt.legend()
plt.show()

# x = [1,2,3,4,5,6,7,8]
# y1 = count2
# plt.xlabel("X-axis")
# plt.ylabel("Y-axis")
# plt.title("A test graph 2")
# for i in range(len(y)):
#     plt.plot(x,[pt for pt in y],label = 'id %s'%i)
# plt.legend()
# plt.show()