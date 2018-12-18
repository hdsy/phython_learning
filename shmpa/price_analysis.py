# -*- coding: utf-8 -*-

#!/usr/bin/python3
import sys

task_name = "all_option"

import traceback

def getBigger(a,b):
    if a>b :
        return a
    else:
        return b

def getSmaller(a,b):
    if a>b :
        return b
    else:
        return a

#更新sku
def CU_sku(db,cur,pname,sname,cc,ec,ct,et):
    
    R_SQL=""
    C_SQL=""
    U_SQL=""

    try:
        R_SQL = "select f_id,f_product_name,f_sku_name,f_customer_check_count,f_engineer_check_count,f_match_count,"+\
            "f_dismatch_count,f_first_time_customer,f_first_time_engineer,f_last_time_customer,f_last_time_engineer from t_second_hand_sku"+\
            " where f_product_name=\""+pname+"\" and f_sku_name=\""+sname +"\""

        if(cc == ec ):
            C_SQL ="insert into t_second_hand_sku" +\
                "(f_product_name,f_sku_name,f_customer_check_count,f_engineer_check_count,f_match_count,f_dismatch_count,f_first_time_customer,"+\
                "f_first_time_engineer,f_last_time_customer,f_last_time_engineer) "+\
                "values(\""+pname+"\",\""+sname+"\",1,1,1,0,\""+ct+"\",\""+et+"\",\""+ct+"\",\""+et+"\") "       
        else:
            C_SQL = "insert into t_second_hand_sku"+\
                " (f_product_name,f_sku_name,f_customer_check_count,f_engineer_check_count,f_match_count,f_dismatch_count,f_first_time_customer,"+\
                "f_first_time_engineer,f_last_time_customer,f_last_time_engineer) "+\
                "values(\""+pname+"\",\""+sname+"\","+str(cc)+","+str(ec)+",0,1,\""+ct+"\",\""+et+"\",\""+ct+"\",\""+et+"\")"
    except:
        print ("Error: string cat  **************")
       

    rfct =""
    rlct =""
    rlet =""
    rfet =""

    cur.execute(R_SQL)
    res1 = cur.fetchone()

    try:
        if res1 != None:             
            rfct = res1["f_first_time_customer"].strftime("%Y-%m-%d %H:%M:%S" )
            rfet = res1["f_first_time_engineer"].strftime("%Y-%m-%d %H:%M:%S" )
            rlct = res1["f_last_time_customer"].strftime("%Y-%m-%d %H:%M:%S" )
            rlet = res1["f_last_time_engineer"].strftime("%Y-%m-%d %H:%M:%S" )     
        else:
            cur.execute(C_SQL)
            db.commit()
            return 

        if(cc == ec ):       
            U_SQL = "update t_second_hand_sku set "+\
                " f_customer_check_count=f_customer_check_count+"+str(cc) +\
                ",f_engineer_check_count=f_engineer_check_count+"+str(ec) +\
                ",f_match_count=f_match_count+1"+\
                ",f_first_time_customer=\""+getSmaller(rfct,ct)+"\""+\
                ",f_first_time_engineer=\""+getSmaller(rfet,et)+"\""+\
                ",f_last_time_customer=\""+getBigger(rlct,ct)+"\""+\
                ",f_last_time_engineer=\""+getBigger(rlet,et)+"\""+\
                " where f_product_name=\""+pname+"\" and" +" f_sku_name=\""+sname+"\""
        else:
            U_SQL = "update t_second_hand_sku set "+\
                " f_customer_check_count=f_customer_check_count+"+str(cc) +\
                ",f_engineer_check_count=f_engineer_check_count+"+str(ec) +\
                ",f_dismatch_count=f_dismatch_count+1"+\
                ",f_first_time_customer=\""+getSmaller(rfct,ct)+"\""+\
                ",f_first_time_engineer=\""+getSmaller(rfet,et)+"\""+\
                ",f_last_time_customer=\""+getBigger(rlct,ct)+"\""+\
                ",f_last_time_engineer=\""+getBigger(rlet,et)+"\""+\
                " where f_product_name=\""+pname+"\" and" +" f_sku_name=\""+sname+"\""

        #print (U_SQL)
    except:
        print ("Error: string cat  **************")
   

    cur.execute(U_SQL)
    db.commit()

        
    return
 
#获取任务
def CR_Process_Log(db,cur,mylist):
    R_SQL = "select f_task_name, f_finished_id ,f_begin_id,f_end_id from t_second_hand_task_log where f_is_work =0 and f_task_name like 'all_option%' order by f_begin_id limit 1"
    
    cur.execute(R_SQL)

    res1 = cur.fetchall()

    if res1 != None:     
        res = res1[0]
        mylist[0] = res["f_begin_id"]
        mylist[1] = res["f_finished_id"]
        mylist[2] = res["f_end_id"]

        global task_name
        task_name = res["f_task_name"]

        print (task_name,"will be assigned ...")

        U_SQL = "update t_second_hand_task_log  set f_is_work = 1  where f_task_name=\""+task_name+"\";"
        cur.execute(U_SQL)
        db.commit()   

      
    
    else:
        print ("All of the tasks have been assigned!")
        
    return 

#更新进度
def U_Process_Log(db,cur,fid):
    

    R_SQL = "select f_finished_id  from t_second_hand_task_log where f_task_name =\"" + task_name  +"\" limit 1"

    U_SQL = "update t_second_hand_task_log  set f_finished_id = "+ str(fid) + " where f_task_name=\""+task_name+"\";"

    cur.execute(R_SQL)

    res1 = cur.fetchall()

    if res1 != None:             

        cur.execute(U_SQL)
        db.commit()     
    
    else:
        print("!!!Error ,you deleted task, when task was still running!", task_name)      

    return     

import pymysql

tar_db = pymysql.connect(host='127.0.0.1',  # 此处必须是是127.0.0.1
                        port=3306,
                        user="root",
                        passwd="test1234",
                        db="hsb_ans", charset='utf8',cursorclass = pymysql.cursors.SSDictCursor)

tar_cursor = tar_db.cursor()               

task_list = [0,0,0]

CR_Process_Log(tar_db,tar_cursor,task_list)

if (task_list[0] >= task_list[2]) or (task_list[0] > task_list[1]) or (task_list[1] >= task_list[2]):
    print ("!!! Task invalid",task_list)
    sys.exit(1)




from sshtunnel import SSHTunnelForwarder

with SSHTunnelForwarder(
        ("119.29.141.207", 22),  # B机器的配置
        ssh_password="bNrq9wNX",
        ssh_username="hsbproxy",
        remote_bind_address=("10.66.117.182", 3306)) as server:  # A机器的配置

   
    # 打开数据库连接
    src_db = pymysql.connect(host='127.0.0.1',  # 此处必须是是127.0.0.1
                                    port=server.local_bind_port,
                                    user="gaoyong",
                                    passwd="uCMNK98a",
                                    db="data_warehouse", charset='utf8',cursorclass = pymysql.cursors.SSCursor)
    
    # 使用 cursor() 方法创建一个游标对象 cursor
   # src_cursor = src_db.cursor()

 
    # 使用 execute()  方法执行 SQL 查询 
    #src_cursor.execute("SELECT VERSION()")
    
    # 使用 fetchone() 方法获取单条数据.
    #data = src_cursor.fetchone()
    
    #print ("Database version : %s " % data)

    #------------------------------------------------------------------------------------------------------------
    # 使用cursor()方法获取操作游标 
    #cursor = db.cursor()
    #src_cursor = pymysql.cursors.SSCursor(src_db)
    src_cursor = src_db.cursor()
    #src_cursor.execute("SET NET_WRITE_TIMEOUT = 60000")
    #src_cursor.execute("SET NET_READ_TIMEOUT = 60000")
    #data = src_cursor.fetchone()
    
 

    # SQL 查询语句
    sql = "SELECT fid, Fproduct_name,Fcheck_product_name,Forder_time,Fcheck_time,Forder_select_desc,fcheck_select_desc " +\
        " FROM t_collection_order_price "+\
        " where fid > " + str(task_list[1]) + " and fid <= " +str(task_list[2]) + " and fcheck_time is true and forder_time is true  "+\
        " order by fid " 

    #/*where Fproduct_id = 30749 limit 100000*/
    try:
        # 执行SQL语句
        print(task_name,sql)
        src_cursor.execute(sql)
        # 获取所有记录列表
        #results = cursor.fetchall()


        row = src_cursor.fetchone()

        dictUserOpion ={}

        while row != None :
        #for row in results:
            dictUserOpion.clear()

            Fid = row[0]

            print (task_name,Fid)

            Fproduct_name= row[1]
            Fcheck_product_name = row[2]
            Forder_time = row[3].strftime("%Y-%m-%d %H:%M:%S" )
            Fcheck_time = row[4].strftime("%Y-%m-%d %H:%M:%S" )

            Forder_select_desc = row[5]
            fcheck_select_desc = row[6]
            

            #拆分字段,
            if Forder_select_desc != None :
                for x in Forder_select_desc.split(','):
                    dictUserOpion[x] =0 # 用户有
                        

            if fcheck_select_desc != None :
              
                for x in fcheck_select_desc.split(','):
                    if x in dictUserOpion:
                        dictUserOpion[x] = 1  #一致
                    else:
                        dictUserOpion[x] =2     #检测有
            
            for  key in dictUserOpion:
                
                if dictUserOpion[key] == 1:
                    CU_sku(tar_db,tar_cursor,Fcheck_product_name,key,1,1,Forder_time,Fcheck_time)

                if dictUserOpion[key] == 2:
                    CU_sku(tar_db,tar_cursor,Fcheck_product_name,key,0,1,Forder_time,Fcheck_time)
                
                if dictUserOpion[key] == 0:
                    CU_sku(tar_db,tar_cursor,Fcheck_product_name,key,1,0,Forder_time,Fcheck_time)

            U_Process_Log(tar_db,tar_cursor,Fid)

            
            row = src_cursor.fetchone()
            

    
    except:    
        traceback.print_exc()
    #------------------------------------------------------------------------------------------------------------

    
    # 关闭数据库连接
    src_db.close()
