## export data into OSI data file
# andy 05/17/13

################################ TO DO ####################################
'''
- takes table and writes out ebcdic
- make view for table automatically/use python to read out

'''
###########################################################################

import sys
import time
import pyodbc as p
import codecs as c
import glob as g
import os as o

#server = 'sfsqldev02v,11433'
#server = 'STSLBXP'
#server = 'DC1SSISDE01V,11433'
#server = '(local)'
server = 'BZNTD21960\\SQL2005'
database = 'WingSpan'

#path = 'I:\\_apstuff\\_FRB\\IN\\'
path = 'K:\\Downloads\\DataScrambling\\OSI\\IN\\'

connStr = ( r'DRIVER={SQL Server};SERVER=' +
            server + ';DATABASE=' + database + ';' +
            'Trusted_Connection=yes'    )

multipleCopybooks = False

fileInfo = [

## SINGLE

#--GOOD to 50 Records
##
('V.NADLYMST.NEW.2100.052413',300),
('V.NWDLYMST.2100.052413',2000),  #2 load errors
('V.ILDLYMST.2100.052413',1500),  #4 load errors
('V.DSDLYBAK.USRDEF.2100.052413',500),
('V.CFDLYMST.2100.052413',100),   #time with no sql insert - 10 min
('V.ALDKYACT.XREF.2100.052413',60),
('V.CFDLYBAK.CFSBL.2100.052413',555), #time with no sql insert - 16 min
('V.MLDLYMST.2100.052413',1500),
('V.MLDLYMST.PAYCHG.2100.052413',500),
('V.ILDLYMST.PMTCG.2100.052413',500), 
('V.SVDLYMST.2100.052413', 2000),  #time with no sql insert - 18 min
('V.ATREQMST.PMSTEX.2100.052413',7111), #WRITE FAILED
('V.TMDLYMST.2100.052413',750),

]
###########################################################################

start_time = time.time()

try:
    for data in fileInfo:
        if(len(data)> 2):
            multipleCopybooks = True
        #print(len(data),multipleCopybooks)    
        print ('File:', data[0])
        table = data[0][:(data[0].rfind('.'))]
        table = table[:(table.rfind('.'))]

        cobolFile= open(path + data[0] + '.EBCDIC', 'wb')

        display = c.getencoder('cp500')
        tableDef = []

        #build layout from staging table..check sum of field lengths against record length

        conn = p.connect(connStr)
        cur = conn.cursor()

        sql = 'select column_name, case when data_type = \'nvarchar\' then \'display\' else \'packed\' end data_type, \
        coalesce(character_maximum_length,(numeric_precision+1)/2,1) field_length, isNull(numeric_scale,0) \
        from information_schema.columns where table_name = \'' + table + '\''
           
        cur.execute(sql)

        previous_length = 0
        rows=0
        for row in cur:
            #make insert statment here
            rows+=1
            tableDef.append((row[0],previous_length,row[2],row[1],row[3]))
            previous_length = previous_length + row[2]

        conn.commit()
        conn.close()
        #print(rows)
        ###########################################################################

        conn = p.connect(connStr)
        cur = conn.cursor()

        if(multipleCopybooks):
            sql = 'select record from [EBCDIC_' + table + '] order by line'
        else:
            sql = 'select top 200 * from [EBCDIC_' + table + ']'
            
        cur.execute(sql)

        for row in cur:
            #print(row)
            for thing in row:
                cobolFile.write(thing)

        cobolFile.close()
        conn.commit()
        conn.close()
    ###########################################################################

except Exception as e: print(e)

    
    
elapsed_time = time.time() - start_time
print('Done',elapsed_time)

