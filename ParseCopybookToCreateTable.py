## script to parse CB and create SQL Server table with CB schema
# open file, read line. if table def, grab name
# if column, grab column
# build create statement one line at a time
# andy 05/03/13

################################ TO DO ####################################
'''

- 01  CF2-RECORD  PIC X(450).
   01  FILLER REDEFINES CF2-RECORD.
- parse assembly language copybooks
------------------------------------EXTRA----------------------------------
- handle complex redefine
- Handle duplicate fields
- Test file for format, has leading numbers?? then define field positions

'''
###########################################################################

import pyodbc as p
import codecs as c
import glob as g
import os as o
import sys

#server = 'sfsqldev02v,11433'
server = 'BZNTD21960\\SQL2005'
#server = 'STSLBXP'
database = 'WingSpan'

location = 'K:\\Downloads\\_25_We_Have\\MKDLYMST\\'
#location = 'C:\\Documents and Settings\\apeterman\\My Documents\\Downloads\\CopyBooks\\copybooksR187-resend\\Misc_Cobol_Copybooks\\'

filefilter = 'CMLMISC.txt'

connStr = ( r'DRIVER={SQL Server};SERVER=' +
            server + ';DATABASE=' + database + ';' +
            'Trusted_Connection=yes'    )

offset = 0
naming = offset + 1
datatype = naming + 2

f = []

def read01(s, fieldList):

    if(len(fieldList) == 3):
        #print ('CREATE TABLE [dbo].[' + fieldList[naming] + '](')
        s = s + 'IF OBJECT_ID(\'[dbo].' + fieldList[naming] + '\', \'U\') IS NOT NULL \n'
        s = s + 'DROP TABLE [dbo].[' + fieldList[naming] + ']\n'        
        s = s + 'CREATE TABLE [dbo].[' + fieldList[naming] + '](\n'
    elif(len(fieldList) == 2):
        s = s + 'IF OBJECT_ID(\'[dbo].' + fieldList[1] + '\', \'U\') IS NOT NULL \n'
        s = s + 'DROP TABLE [dbo].[' + fieldList[1] + ']\n'
        s = s + 'CREATE TABLE [dbo].[' + fieldList[1] + '](\n'
    return s

def read05(s, fieldList):
    #print(' '.join(fieldList))
    datatype = 3
    if('COMP-3' in fieldList):
        if('REDEFINES' in fieldList):
            datatype = datatype +2
            s=s+'--'
            #print('naming: ' + str(naming))
        #print ('[' + fieldList[naming] + '] [decimal](' + fieldList[datatype] + ', ' + fieldList[datatype] + ') NULL,')

        #fieldList[naming] = fieldList[naming][fieldList[naming].find('-')+1:]
        
        if(fieldList[datatype] == 'S999V9(08)'):                
            s = s + '\t[' + fieldList[naming] + '] [decimal](11, 8) NULL,\n'
                   
        elif(fieldList[datatype] == 'S99V9(03)'):                
            s = s + '\t[' + fieldList[naming] + '] [decimal](5, 3) NULL,\n'
            
        elif(fieldList[datatype] in ['S9(03)V9(08)','S9(3)V9(8)']):                
            s = s + '\t[' + fieldList[naming] + '] [decimal](11, 8) NULL,\n'
            
        elif(fieldList[datatype] == 'S9(11)V9(02)'):                
            s = s + '\t[' + fieldList[naming] + '] [decimal](13, 2) NULL,\n'
            
        elif(fieldList[datatype] == 'S9(09)V9(4)'):                
            s = s + '\t[' + fieldList[naming] + '] [decimal](13, 4) NULL,\n'
            
        elif(fieldList[datatype] == 'S9(02)V9'):                
            s = s + '\t[' + fieldList[naming] + '] [decimal](3, 1) NULL,\n'
            
        elif(fieldList[datatype] == 'SV9(11)'):   
            s = s + '\t[' + fieldList[naming] + '] [decimal](11, 0) NULL,\n'
            
        elif(fieldList[datatype] == 'SV9(01)'):            
            s = s + '\t[' + fieldList[naming] + '] [decimal](1, 1) NULL,\n'

        elif(fieldList[datatype] == 'S999V9999'):                
            s = s + '\t[' + fieldList[naming] + '] [decimal](7, 4) NULL,\n'
            
        elif(fieldList[datatype] == 'S999V99'):                
            s = s + '\t[' + fieldList[naming] + '] [decimal](5, 2) NULL,\n'
                             
        elif(fieldList[datatype] == 'S99V99999'):
            s = s + '\t[' + fieldList[naming] + '] [decimal](7, 5) NULL,\n'
            
        elif(fieldList[datatype] == 'S99V999'):
            s = s + '\t[' + fieldList[naming] + '] [decimal](5, 3) NULL,\n'
            
        elif(fieldList[datatype] == 'S9V9999'):            
            s = s + '\t[' + fieldList[naming] + '] [decimal](1, 1) NULL,\n'
            
        elif(fieldList[datatype] == 'S999'):            
            s = s + '\t[' + fieldList[naming] + '] [decimal](3, 0) NULL,\n'
            
        elif(fieldList[datatype] == 'S9'):   
            s = s + '\t[' + fieldList[naming] + '] [decimal](1, 0) NULL,\n'
            
        elif(fieldList[datatype].find('V99')>0):                
            s = s + '\t[' + fieldList[naming] + '] [decimal](' \
            + str(int(((fieldList[datatype][(fieldList[datatype].find('('))+1:(fieldList[datatype].find(')'))]).lstrip('0'))) \
            + 2) + ', 2) NULL,\n'
            
        elif(fieldList[datatype].find('V9(')>0):                
            s = s + '\t[' + fieldList[naming] + '] [decimal](' \
            + (fieldList[datatype][(fieldList[datatype].find('('))+1:(fieldList[datatype].find(')'))]).lstrip('0') \
            + ', 2) NULL,\n'
            
        else:
            s = s + '\t[' + fieldList[naming] + '] [decimal](' \
            + (fieldList[datatype][(fieldList[datatype].find('('))+1:(fieldList[datatype].find(')'))]).lstrip('0') \
            + ', 0) NULL,\n'
            
        s = s + '\t[' + fieldList[naming] + '_sign] [bit] NULL,\n'
        #print(fieldList[naming])

            
    elif(len(fieldList) == offset+2): # has subfields, add them
        #print(fieldList[naming])
        #fieldList[naming] = fieldList[naming][fieldList[naming].find('-')+1:]
        size = 0
        s = s + '\t[' + fieldList[naming] + '] [nvarchar]' 
        while True:
            level10 = f.readline()
            breakdown = level10[:level10.find('.')].split()
            #print(' '.join(breakdown))            
            if(breakdown[0] != '*'):
                if(breakdown[offset] == '10'): #add support for 15s
                    size = size + len(breakdown[datatype])
                elif(breakdown[offset] == '05'):
                    s = s + '(' + str(size) + ') NULL,\n'
                    #s = s + breakdown[2] + '\n'
                    print('AFTER 10 '+ breakdown[naming])
                    s = read05(s,breakdown);                
                    break
                else:
                    s = s + '(' + str(size) + ') NULL,\n'
                    break

    else:
        #print(fieldList[naming])
        #print ('[' + fieldList[naming] + '] [nvarchar](' + fieldList[datatype][2:4] + ') NULL,')
        #fieldList[naming] = fieldList[naming][fieldList[naming].find('-')+1:]
        if(fieldList[datatype].find('(') > 0):
            s = s + '\t[' + fieldList[naming] + '] [nvarchar](' \
            + (fieldList[datatype][(fieldList[datatype].find('('))+1:(fieldList[datatype].find(')'))]).lstrip('0') + ') NULL,\n'            
        else:
            #print ('[' + fieldList[naming] + '] [nvarchar]' + ' NULL,\n')
            s = s + '\t[' + fieldList[naming] + '] [nvarchar](' + str(len(fieldList[datatype])) + ') NULL,\n'
            
    return s

###########################################################################

o.chdir(location)

for files in g.glob(filefilter):
    print ('--' + files)
    #try:
    f = open(location + files, 'r')

    sql = ''

    if(files[:1] == 'C'):
        
        print('--Parsing Cobol Copybook')
        
        for line in f:
            #print(line)
            goodpart = line[:line.find('.')] #Take up to period
            #print (goodpart)
            fields = goodpart.split()

            #Table 
            if ((' 01 ' in line) and (line.find('.') > 0)) and ('*' not in fields[0]) and ('88' not in fields[0]):                
                sql = read01(sql,fields);
            elif ((line.find('.') > 0)) and ('*' not in fields[0]) and ('88' not in fields[0]):
                #print(line)
                try:
                    #naming = offset + 1
                    sql = read05(sql,fields);
                except:
                    print('Bad Parse')
                
    elif(files[:1] == '$'):
        
        print('--Parsing Assembler Copybook')

        for line in f:
            #print(line)
            goodpart = line[:line.find('.')] #Take up to period
            #print (goodpart)
            fields = goodpart.split()

            #Table 
            if ((' 01 ' in line) and (line.find('.') > 0)) and ('*' not in fields[0])and ('88' not in fields[0]):                
                sql = read01(sql,fields);
            elif (' 05 ' in line) and ('*' not in fields[0]) and ('88' not in fields[0]):                
                sql = read05(sql,fields);
        
    sql = sql[:-2]
    sql = sql + '\n) ON [PRIMARY]'

    if('CREATE TABLE' in sql):
        print (sql)
    else:
        print('Invalid SQL Statement')
        print(sql)

    #conn = p.connect(connStr)
    #cur = conn.cursor()

    #cur = conn.cursor()
    #cur.execute(sql)
    #conn.commit()
    #conn.close()
        
###########################################################################
    #except:
        print('Problem parsing file.')



rint('Problem parsing file.')
