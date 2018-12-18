# -*- coding: utf-8 -*-

#!/usr/bin/python3

#获取任务进度
def CR_Process_Log(db,cur,fid,tid):
    task_name = "all_option"+"_" + str(fid)+"_" + str(tid) 

    cur_id_ed = 0

    R_SQL = "select f_finished_id , f_is_work from t_second_hand_task_log where f_task_name =\"" +task_name + "\""

    C_SQL = "insert into t_second_hand_task_log(f_task_name,f_finished_id,f_is_work,f_begin_id,f_end_id,f_create_time,f_modify_time) values(\"" + task_name + "\","+str(fid)+",0,"+str(fid)+"," + str(tid) +",now(),now());"

    cur.execute(R_SQL)

    res = cur.fetchone()

    if res != None:     
        
        cur_id_ed = res["f_finished_id"]

        print (f" task {task_name} is already exist . ")
     
    
    else:
        print (C_SQL)
        cur.execute(C_SQL)
        db.commit()
        

    return cur_id_ed

import pymysql

tar_db = pymysql.connect(host='127.0.0.1',  # 此处必须是是127.0.0.1
                        port=3306,
                        user="root",
                        passwd="test1234",
                        db="hsb_ans", charset='utf8',cursorclass = pymysql.cursors.SSDictCursor)

tar_cursor = tar_db.cursor()         


def Task_Assign(beginid,endid,step):
    tb = beginid
    te = endid

    while (te>tb) :
             
        if (te-tb <= step):
            print(f"task is {tb} - {te} ")
            CR_Process_Log(tar_db,tar_cursor,tb,te)
            return
        else:
            print(f"task is {tb} - {tb+step }" )
            CR_Process_Log(tar_db,tar_cursor,tb,tb+step)
            tb = tb + step +1

    return





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
    
    src_cursor = src_db.cursor()

    src_cursor.execute("select max(fid) as max_fid,min(fid) as min_fid from t_collection_order_price")
    data = src_cursor.fetchone()
    
    if data != None :
    
        max = data[0]
        min = data[1]

        step = 200000

        print (max ,min, step)

        Task_Assign(min,max,step)

        

    else:
        print("Error in open src database.")

    src_cursor.close()

    src_db.close()


tar_cursor.close()
tar_db.close()

print ("this is the end")