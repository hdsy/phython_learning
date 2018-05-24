# -*- coding: utf-8 -*-

#!/usr/bin/python3
 
import pymysql

dictOption = {}
dictOption ['count'] = 1

dictOptionUser ={}
dictOptionUser ['count'] = 1

dictOptionAct = {}
dictOptionAct ['count'] = 1
 
# 打开数据库连接
db = pymysql.connect("localhost","root","root1234","db_recycle_core", charset='utf8',cursorclass = pymysql.cursors.SSDictCursor)
 
# 使用 cursor() 方法创建一个游标对象 cursor
cursor = db.cursor()

 
# 使用 execute()  方法执行 SQL 查询 
cursor.execute("SELECT VERSION()")
 
# 使用 fetchone() 方法获取单条数据.
data = cursor.fetchone()
 
print ("Database version : %s " % data)

#------------------------------------------------------------------------------------------------------------
# 使用cursor()方法获取操作游标 
#cursor = db.cursor()
cursor = pymysql.cursors.SSCursor(db)
 
# SQL 查询语句
sql = "SELECT forder_select_desc,fcheck_select_desc FROM t_collection_order_price limit 10000000"

#/*where Fproduct_id = 30749 limit 100000*/
try:
    # 执行SQL语句
    cursor.execute(sql)
    # 获取所有记录列表
    #results = cursor.fetchall()

    row = cursor.fetchone()

    while row != None :
    #for row in results:
        user_option = row[0]
        act_option = row[1]
        

        #拆分字段,
        if user_option != None :
            dictOption ['count'] +=1
            dictOptionUser ['count'] +=1
            for x in user_option.split(','):
                if x in dictOption:
                    dictOption[x] += 1
                else:
                    dictOption[x] =1
                
                if x in dictOptionUser:
                    dictOptionUser[x] += 1
                else:
                    dictOptionUser[x] =1
                    

        if act_option != None :
            dictOption ['count'] +=1
            dictOptionAct ['count'] +=1
            for x in act_option.split(','):
                if x in dictOption:
                    dictOption[x] += 1
                else:
                    dictOption[x] =1
                
                if x in dictOptionAct:
                    dictOptionAct[x] += 1
                else:
                    dictOptionAct[x] =1
        
        # 打印结果
        #print ('.')
        #print ("user_option=%s, 对比  ,act_option=%s" % \
        #       (user_option,act_option ))

        row = cursor.fetchone()
         


except:
   print ("Error: unable to fetch data")
#------------------------------------------------------------------------------------------------------------

 
# 关闭数据库连接
db.close()

#打印选项表

print (dictOption)

f = open("./option_all.txt",'w')
f.write(str(dictOption))
f.close()


print (dictOptionUser)
f = open("./option_user.txt",'w')
f.write(str(dictOptionUser))
f.close()


print (dictOptionAct)
f = open("./option_act.txt",'w')
f.write(str(dictOptionAct))
f.close()