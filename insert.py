import csv
import psycopg2
import os
import pandas as pd
import time

table_master = [[0,0] for i in range(3)] # data which will be written in master table is written here
table_slave_niranjan = [[0,0] for i in range(3)] # data which will be written in Niranjan's table is written here
table_slave_sampat = [[0,0] for i in range(3)] # data which will be written in Sampat's Desktop table is written here
table_slave_sampat_l = [[0,0] for i in range(3)] # data which will be written in Sampat's Laptop table is written here

start_time = time.time() #this is used to calculate elapsed time for insertion data into the servers

print("Connecting to all the servers")

pid1 = 2347 #port number
pid2 = 5434 #port number
sD = []
sL = []
nD = []
cur_sD = []
cur_sL = []
cur_nD = []

#establishing connection with all the servers in Sampat's Desktop
for i in range (3):
	sD.append(psycopg2.connect(host="Enter IP Here",port=pid1, database="postgres", user="postgres", password="it701"))
	pid1 -= 1

#establishing connection with all the servers in Sampat's Laptop
for i in range (3):
	nD.append(psycopg2.connect(host="Enter IP Here",port=pid2, database="postgres", user="postgres", password="it701"))
	pid2 -= 1

pid2 += 3

#establishing connection with all the servers in Niranjan's Desktop
for i in range (3):
	sL.append(psycopg2.connect(host="Enter IP Here",port=pid2, database="postgres", user="postgres", password="it701"))
	pid2 -= 1

print("All servers are connected")

#assigning cursors
for i in range (3):
	cur_sD.append(sD[i].cursor())
for i in range (3):
	cur_sL.append(sL[i].cursor())
for i in range (3):
	cur_nD.append(nD[i].cursor())

#cleaning tables
for i in range (3):
	cur_sD[i].execute("TRUNCATE TABLE dictionary1{};".format(i+1))
for i in range (3):
	cur_sL[i].execute("TRUNCATE TABLE dictionary2{};".format(i+1))
for i in range (3):
	cur_nD[i].execute("TRUNCATE TABLE dictionary3{};".format(i+1))


print("Partitioning Database...")

#writing data into all the servers
with open('diction.csv','r') as f: #taking data from database found at wordnet.princton.edu
	reader = csv.reader(f)
	count = 1
	# storing rows by sending queries to appropriate servers
	for row in reader:
		if count <= 19531:
				cur_sD[0].execute("INSERT INTO dictionary11 VALUES (%s, %s)",row)
		elif count <= 39062:
				cur_sD[1].execute("INSERT INTO dictionary12 VALUES (%s, %s)",row)
		elif count <= 58593:
				cur_sD[2].execute("INSERT INTO dictionary13 VALUES (%s, %s)",row)
		elif count <= 78124:
				cur_sL[0].execute("INSERT INTO dictionary21 VALUES (%s, %s)",row)
		elif count <= 97665:
				cur_sL[1].execute("INSERT INTO dictionary22 VALUES (%s, %s)",row)
		elif count <= 117186:
				cur_sL[2].execute("INSERT INTO dictionary23 VALUES (%s, %s)",row)
		elif count <= 136717:
				cur_nD[0].execute("INSERT INTO dictionary31 VALUES (%s, %s)",row)
		elif count <= 156248:
				cur_nD[1].execute("INSERT INTO dictionary32 VALUES (%s, %s)",row)
		else:
				cur_nD[2].execute("INSERT INTO dictionary33 VALUES (%s, %s)",row)

		#storing map values better efficient processing
		if count == 1:
			table_master[0][0] = str(row[0])
			table_slave_sampat[0][0] = str(row[0])
			print("Inserting Data in Server 1 in Sampat's Desktop")
		elif count == 19531:
			table_slave_sampat[0][1] = str(row[0])
			print("Done")
		elif count == 19532:
			table_slave_sampat[1][0] = str(row[0])
			print("Inserting Data in Server 2 in Sampat's Desktop")
		elif count == 39062:
			table_slave_sampat[1][1] = str(row[0])
			print("Done")
		elif count == 39063:
			table_slave_sampat[2][0] = str(row[0])
			print("Inserting Data in Server 3 in Sampat's Desktop")
		elif count == 58593:
			table_master[0][1] = str(row[0])
			table_slave_sampat[2][1] = str(row[0])
			print("Done")
		elif count == 58594:
			table_master[1][0] = row[0]
			table_slave_sampat_l[0][0] = str(row[0])
			print("Inserting Data in Server 1 in Sampat's Laptop")
		elif count == 78124:
			table_slave_sampat_l[0][1] = str(row[0])
			print("Done")
		elif count == 78125:
			table_slave_sampat_l[1][0] = str(row[0])
			print("Inserting Data in Server 2 in Sampat's Laptop")
		elif count == 97665:
			table_slave_sampat_l[1][1] = str(row[0])
			print("Done")
		elif count == 97666:
			table_slave_sampat_l[2][0] = str(row[0])
			print("Inserting Data in Server 3 in Sampat's Laptop")
		elif count == 117186:
			table_master[1][1] = row[0]
			table_slave_sampat_l[2][1] = str(row[0])
			print("Done")
		elif count == 117187:
			table_master[2][0] = row[0]
			table_slave_niranjan[0][0] = str(row[0])
			print("Inserting Data in Server 1 in Niranjan's Laptop")
		elif count == 136717:
			table_slave_niranjan[0][1] = str(row[0])
			print("Done")
		elif count == 136718:
			table_slave_niranjan[1][0] = str(row[0])
			print("Inserting Data in Server 2 in Niranjan's Laptop")
		elif count == 156248:
			table_slave_niranjan[1][1] = str(row[0])
			print("Done")
		elif count == 156249:
			table_slave_niranjan[2][0] = str(row[0])
			print("Inserting Data in Server 3 in Niranjan's Laptop")
		elif count == 175782:
			table_master[2][1] = str(row[0])
			table_slave_niranjan[2][1] = str(row[0])
			print("Done")
		count = count + 1	

#closing cursors
for i in range (3):
	cur_sD[i].close()
for i in range (3):
	cur_sL[i].close()
for i in range (3):
	cur_nD[i].close()

#commiting the connection
for i in range (3):
	sD[i].commit()
for i in range (3):
	sL[i].commit()
for i in range (3):
	nD[i].commit()

#closing the connections
for i in range (3):
	sD[i].close()
for i in range (3):
	sL[i].close()
for i in range (3):
	nD[i].close()

print("Time taken to fragment data into all the servers = {} seconds".format(time.time()-start_time))

#write map table
df1 = pd.DataFrame(table_master)
df1.to_csv('master_table.csv', header = False, index=False) 

df2 = pd.DataFrame(table_slave_sampat)
df2.to_csv('slave_table_sampat_desktop.csv', header = False, index=False) 

df3 = pd.DataFrame(table_slave_sampat_l)
df3.to_csv('slave_table_sampat_laptop.csv', header = False, index=False) 

df4 = pd.DataFrame(table_slave_niranjan)
df4.to_csv('slave_table_niranjan_desktop.csv', header = False, index=False) 
