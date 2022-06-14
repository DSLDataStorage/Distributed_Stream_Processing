import sys
import re

fin = open(sys.argv[1], 'r')
fout = open("avr.out", 'w')

#time = []
#for i in range(10):
#	time.append([0]*100)

time = [0,0,0,0,0,0,0,0,0,0]

flag = 0

while 1:
	line = fin.readline()
	data = line.split('[')
	if len(data) == 2:
		data = data[1].split(']')
		if data[0] == "11": break

count = 1
while 1:
	line = fin.readline()
	if not line: break
	
	if re.match("=", line) != None:
		count = count + 1
	data = line.split(']')
	if len(data) == 2:
		data = data[0].split('_')
		#if re.match("[T0-9]", data[0]) != None:
		if len(data) == 2:
			time_index = int(data[1]) - 1
			time_ms = line.split(' ')[-2]
			time[time_index] = time[time_index] + int(time_ms)
			#time[time_index].append(time_ms)
			#print time_index

num = 1
for i in time:
	avr = i / count
	fout.write("[T" + str(num) + "] " + str(avr) + " ms\n")
	num = num + 1

