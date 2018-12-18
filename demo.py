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
            "values(\""+pname+"\",\""+sname+"\","+cc+","+ec+",0,1,\""+ct+"\",\""+et+"\",\""+ct+"\",\""+et+"\")"

       

    rfct =""
    rlct =""
    rlet =""
    rfet =""

   

    if(cc == ec ):       
        U_SQL = "update t_second_hand_sku set "+\
            " f_customer_check_count=f_customer_check_count+"+str(cc) +\
            ",f_engineer_check_count=f_engineer_check_count+"+str(ec) +\
            ",f_match_count=f_match_count+1"+\
            ",f_first_time_customer=\""+getSmaller(rfct,ct)+"\""+\
            ",f_first_time_engineer=\""+getSmaller(rfet,et)+"\""+\
            ",f_last_time_customer=\""+getBigger(rlct,ct)+"\""+\
            ",f_first_time_engineer=\""+getBigger(rlet,et)+"\""+\
            " where f_product_name=\""+pname+"\" and" +"f_sku_name=\""+sname+"\""
    else:
        U_SQL = "update t_second_hand_sku set "+\
            " f_customer_check_count=f_customer_check_count+"+cc +\
            ",f_engineer_check_count=f_engineer_check_count+"+ec +\
            ",f_dismatch_count=f_dismatch_count+1"+\
            ",f_first_time_customer=\""+getSmaller(rfct,ct)+"\""+\
            ",f_first_time_engineer=\""+getSmaller(rfet,et)+"\""+\
            ",f_last_time_customer=\""+getBigger(rlct,ct)+"\""+\
            ",f_first_time_engineer=\""+getBigger(rlet,et)+"\""+\
            " where f_product_name=\""+pname+"\" " +"f_sku_name=\""+sname+"\""

    print (U_SQL)
   
   # cur.execute(U_SQL)
   # db.commit()

        
    return


CU_sku(1,1,"a","b",1,1,"2017-04-14","2017-04-24")