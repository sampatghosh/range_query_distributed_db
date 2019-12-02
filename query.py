import csv
import os
import sys
import time
import psycopg2
import pandas as pd

def sampat_desktop(low,high):

    with open('slave_table_sampat_desktop.csv','r') as f:
        reader = csv.reader(f)
        mylist = list(reader)

    count = 1
    pid = 2347
    sD = []
    cur_sD = []
    out = []
    i = 0
    db = 1
    count = 0

    #iterating through all the rows in table
    for row in mylist: 
        if low < row[0]:
            break
        elif low >= row[0] and low <= row[1]:
        	#establishing connection
            sD.append(psycopg2.connect(host="Enter IP Here",port=pid, database="postgres", user="postgres", password="it701"))
            print("     Going to Server {} with port {}".format(db,pid))
            cur_sD.append(sD[i].cursor())

            #sending query to server i for execution
            cur_sD[i].execute("select * from dictionary1{} where word between '{}' and '{}' order by word;".format(db,low,high))
            
            #storing output
            out.append(cur_sD[i].fetchall())
            
            #storing output in dataframe 
            df = pd.DataFrame(out[i])
            
            #saving output in output file
            df.to_csv('output.csv', mode = 'a', header = False, index=False)
            
            # calculating no. of entries that came in output
            count += df.shape[0] 
            
            # range extends to next server in same machine
            if high > str(row[1]) and db < len(mylist): 
                low = str(mylist[db][0])
            i += 1
        pid -= 1
        db +=1

    return count #returning no. of entries that came as output

def sampat_laptop(low,high):

    with open('slave_table_sampat_laptop.csv','r') as f:
        reader = csv.reader(f)
        mylist = list(reader)

    count = 1
    pid = 5434
    sL = []
    cur_sL = []
    out = []
    i = 0
    db = 1
    count = 0

    #iterating through all the rows in table
    for row in mylist: 
        if low < row[0]:
            break
        elif low >= row[0] and low <= row[1]:
            print("     Going to Server {} with port {}".format(db,pid))
            
            #establishing connection
            sL.append(psycopg2.connect(host="Enter IP Here",port=pid, database="postgres", user="postgres", password="it701"))
            cur_sL.append(sL[i].cursor())
            
            #sending query to server i for execution
            cur_sL[i].execute("select * from dictionary2{} where word between '{}' and '{}' order by word;".format(db,low,high))
            
            #storing output
            out.append(cur_sL[i].fetchall())
            
            #storing output in dataframe 
            df = pd.DataFrame(out[i])
            
            #saving output in output file
            df.to_csv('output.csv', mode = 'a', header = False, index=False)

            # calculating no. of entries that came in output
            count += df.shape[0]
            
            # range extends to next server in same machine
            if high > row[1] and db < len(mylist):
                low = str(mylist[db][0])
            i += 1
        pid -= 1
        db += 1

    return count #returning no. of entries that came as output


def niranjan_desktop(low,high):

    with open('slave_table_niranjan_desktop.csv','r') as f:
        reader = csv.reader(f)
        mylist = list(reader)

    count = 1
    pid = 5434
    nD = []
    cur_nD = []
    out = []
    i = 0
    db = 1
    count = 0

    for row in mylist:
        if low < row[0]:
            break
        elif low >= row[0] and low <= row[1]:
            print("     Going to Server {} with port {}".format(db,pid))
            
            #establishing connection
            nD.append(psycopg2.connect(host="Enter IP Here",port=pid, database="postgres", user="postgres", password="it701"))
            cur_nD.append(nD[i].cursor())
            
            #sending query to server i for execution
            cur_nD[i].execute("select * from dictionary3{} where word between '{}' and '{}' order by word;".format((i+1),low,high))
            
            #storing output
            out.append(cur_nD[i].fetchall())

            #storing output in dataframe 
            df = pd.DataFrame(out[i])

            #saving output in output file
            df.to_csv('output.csv', mode = 'a', header = False, index=False)

            #saving output in output file
            count += df.shape[0]

            # range extends to next server in same machine
            if high > row[1] and db < len(mylist):
                low = str(mylist[db][0])
            i += 1
        pid -= 1
        db += 1

    return count #returning no. of entries that came as output

low = "Z"
high = "A"
while low > high:
    low = str(input("Enter the low range: "))
    high = str(input("Enter the high range: "))

# cleaning output file
with open('output.csv','w') as f: 
    f.truncate()
    f.close()

#using time.time() to calculate elapsed time for a query
start_time = time.time()

loc = ["Sampat's Desktop","Sampat's Laptop","Niranjan's Desktop"]

with open('master_table.csv','r') as f:
    reader = csv.reader(f)
    mylist = list(reader)

i = 0
count = 0

# iterating through all the rows in master table
for row in mylist: 
    if low >= row[0] and low <= row[1]: # if lower range is present in machine loc[i]

        if high <= str(row[1]): # if range is within only one machine
    
            print("Going to {}".format(loc[i]))
    
            if i == 0:
            	#returns the no. of entries present within low and high and also writes the output in output.csv
                count += sampat_desktop(low,high) 
            elif i == 1:
            	#returns the no. of entries present within low and high and also writes the output in output.csv
                count += sampat_laptop(low,high) 
            else:
            	#returns the no. of entries present within low and high and also writes the output in output.csv
                count += niranjan_desktop(low,high) 
            break
    
        else: # if range is in multiple machines
    
            print("Going to {}".format(loc[i])) 
    
            if i == 0:
            	#returns the no. of entries present within low and row[1] and also writes the output in output.csv
                count += sampat_desktop(low,row[1]) 
            elif i == 1:
            	#returns the no. of entries present within low and row[1] and also writes the output in output.csv
                count += sampat_laptop(low,row[1]) 
            else:
            	#returns the no. of entries present within low and row[1] and also writes the output in output.csv
                count += niranjan_desktop(low,row[1]) 
    
            if(i < len(mylist)-1):
                low = mylist[i+1][0]
    
    i += 1

print("Total no. of entries= {}".format(count))

print("Time taken to execute queries = {} milliseconds".format((time.time()-start_time)*1000))
