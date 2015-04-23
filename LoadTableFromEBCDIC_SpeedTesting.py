## script to decode and load a EBCDIC file into the database
# open file, read line. if table def, grab name
# if column, grab column
# andy 05/03/13

################################ TO DO ####################################
'''
- change insert to be bigger chunk, or all at once

'''
###########################################################################

import sys
import math
import datetime
import time
import pyodbc as p
import gc
import codecs as c
import glob as g
import os as o

server = 'BZNTD21960\\SQL2005'
database = 'WingSpan'

path = 'K:\\Downloads\\DataScrambling\\OSI\\IN\\'

connStr = ( r'DRIVER={SQL Server Native Client 10.0};SERVER=' +
            server + ';DATABASE=' + database + ';' +
            'Trusted_Connection=yes'    )

fileInfo = [

('V.MLDLYMST.PAYCHG.2100.052413',500),
('V.MLDLYMST.PAYCHG.2100.052413_5000',500),
('V.MLDLYMST.PAYCHG.2100.052413_5000',500),
('V.MLDLYMST.PAYCHG.2100.052413_5000',500),
]

debugging = 0
debugrecords = 2

display = c.getdecoder('cp500')

def yieldRecords( aFile, recSize ):
    recBytes= aFile.read(recSize)
    while recBytes:
        yield recBytes
        recBytes= aFile.read(recSize)


def getPiece( aFile, reclen, chunk, iteration ):

    cobolFile= open(aFile, 'rb')
    cobolFile.seek(iteration*(reclen*chunk))
    cobolFilePiece = cobolFile.read(reclen*chunk)
    cobolFile.close()
    return cobolFilePiece

        
def yieldRecords2( aFilePiece, recSize ):
    i=0
    recBytes= aFilePiece[:recSize]
    print(len(recBytes))
    while recBytes:
        yield recBytes
        i+=1
        recBytes= aFilePiece[i*recSize:(i+1)*recSize]
        
def packed(bytes,p):
    n= [ '' ]
    #print(bytes[:-1])
    for b in bytes[:-1]:
        hi, lo = divmod(ord(chr(b)), 16)
        n.append( str(hi) )
        n.append( str(lo) )
    digit, sign = divmod(ord(chr(bytes[-1])), 16)
    if(p==1):
        n.append( str(digit) ) # don't append this if field is an even number
    if sign in (0x0b,0x0d):
        n[0]= '-'
    elif sign in (0x0c,0x0f):
        n[0]= '+'
    else:        
         print('BAD DECIMAL')  
    i = int(str(''.join(n)))
    return (sign,i)

def load(s,r):

    conn1 =p.connect(connStr)
    conn1.autocommit = False
    cur1 = conn1.cursor()
    cur1.executemany(s,r)
    conn1.commit()
    conn1.close()
    
    return

###########################################################################

start_time = time.time()

#build layout from staging table..check sum of field lengths against record length
chunkSize = 5000
for data in fileInfo:
    print ('File:', data[0])
    
    table = data[0][:(data[0].rfind('.'))]
    table = table[:(table.rfind('.'))]
    recordLen = data[1]

    #get size of table
    conn = p.connect(connStr)
    cur = conn.cursor()
    cur.execute('select sum(coalesce(character_maximum_length,ceiling((numeric_precision+1)/2.0))) table_length \
    from information_schema.columns where table_name = \'' + table + '\'')
                
    row = cur.fetchone()
    print ('Table:', table)
    print ('Length of Data = ' + str(recordLen))
    print ('Length of Table = ' + str(row[0]))

    #get column information of table
    sql = 'select column_name, case when data_type in (\'nvarchar\') then \'display\' when data_type = \'bit\' then \'sign\' else \'packed\' end data_type, \
    coalesce(character_maximum_length,cast(ceiling((numeric_precision+1)/2.0)as int),1) field_length, numeric_precision%2 parity, isNull(numeric_scale,0) \
    from information_schema.columns where table_name = \'' + table + '\''
    #print(sql)   
    cur.execute(sql)

    tableDef = []
    previous_length = 0

    sqlInsert = 'insert into [' + table + ']('
    sqlValues = 'values ('
    for row in cur:
        #make insert statment here
        sqlInsert = sqlInsert + '[' + row[0] + '],'
        sqlValues = sqlValues + ' ?,'
        #make table def here
        if(row[1] != 'sign'):
            tableDef.append((row[0],previous_length, row[2],row[1],row[3],row[4]))
            previous_length = previous_length + row[2]

    sqlInsert = sqlInsert[:-1] + ')'
    sqlValues = sqlValues[:-1] + ')'

    sql = sqlInsert + '\n' + sqlValues
    #print(sql)
    conn.commit()
    conn.close()

    ###########################################################################

    conn = p.connect(connStr)
    cur = conn.cursor()
    inserted = 0
    failed = 0
    records = []
    rows = []
    chunks = []

    #bigData = cobolFile.read
    #each record
    fileName = path + data[0]
    fileSize = o.stat(fileName).st_size
    loops = math.ceil(fileSize/(recordLen*chunkSize))
    cobolFilePiece = getPiece(fileName,recordLen, chunkSize,0)
    #print(loops)
    for l in range(loops):
        print('l',l,'loops',loops)

    
        #print(len(cobolFilePiece))
        for recBytes in yieldRecords2(cobolFilePiece, recordLen):
            record = dict()
            row = []
            f = 0

            if(table[-3:] =='RAW'):
                row.append(str(recBytes))
            else:    
                #each field
                for name, start, size, convert, parity, factor in tableDef:
                    f+=1
                    if(convert =='display'):
                        if(debugging==1):
                            print(f,'nvarchar',recBytes[start:start+size])            
                        record[name]= display(recBytes[start:start+size])
                        if(debugging==1):
                            print(size,record[name][0]+ ' '+ ascii(record[name][0]) +'\n')
                        row.append(record[name][0])
                    elif(convert =='packed'):
                        f+=1
                        if(debugging==1):
                            print(f,'decimal',recBytes[start:start+size])
                        sign,record[name]= packed(recBytes[start:start+size],parity)
                        row.append(str((int(record[name])/(pow(10,factor)))))
                        if(debugging==1):
                            print(size, str((int(record[name])/(pow(10,factor))))+'\n')
                        if(sign ==15):
                            row.append(1)
                        else:
                            row.append(0)

            if('TRLRMLT_MO' not in sql):
                try:
                    inserted+=1
                    #cur.execute(sql,row)
                    rows.append(row)
                except:
                    print('BAD SQL')
                    print(row)
                    failed+=1
                    debugging = 1
        ##    print(row)        
        ##    cur.execute(sql,row)
                        
            if(debugging==1):
                if((inserted+failed)> debugrecords):
                    conn.commit()
                    break
            if((inserted+failed)%chunkSize==0):
                elapsed_time = time.time() - start_time    
                print('Done parsing...',inserted+failed,'records', str(datetime.timedelta(seconds=elapsed_time)))
                #print(sql,len(rows))
                #load(sql,rows)
                #rec = list(rows)
                #chunks.append(rec)
                #print('appending rows to chunks', len(rec),len(chunks))
                cur.executemany(sql,rows)
                elapsed_time = time.time() - start_time   
                print('Done inserting...',inserted+failed,'records', str(datetime.timedelta(seconds=elapsed_time)))
                rows.clear()
                
        #add the extra data
        if(len(rows)>0):
            cur.executemany(sql,rows)
            #rec = list(rows)
            #chunks.append(rec)
            
    conn.commit()       
    conn.close()

elapsed_time = time.time() - start_time            
print('Done parsing.',str(datetime.timedelta(seconds=elapsed_time)))

###########################################################################

elapsed_time = time.time() - start_time
print('Done.',str(datetime.timedelta(seconds=elapsed_time)))
