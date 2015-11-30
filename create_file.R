library("RPostgreSQL", lib.loc="~/R/win-library/3.2")

drv = dbDriver("PostgreSQL")
con_psql = dbConnect(drv,host="172.25.197.164",user="gpadmin",password="gpadmin",dbname="blade_summary_test_db")

cate_data = dbGetQuery(conn=con_psql, "select level1 from google_vertical_master_level group by level1 order by level1")
rows=nrow(cate_data)
hashmap = new.env(hash = TRUE, size = rows)
val_tbl=rep(0,rows)
out.data=as.data.frame(t(append("",val_tbl)))

for(i in 1:rows){
  hashmap[[as.character(cate_data[i,1])]] = i
}

rs=dbSendQuery(conn = con_psql,"select reader_id, vertical_id, sum(total_ac) as access from google_ana_data_level1 group by reader_id, 
               vertical_id order by reader_id, vertical_id")
data=fetch(rs,1)
rows=nrow(data)
pre_reader=data[1,1]
count=0
idx=0
mod_base=100

while(rows>=0){
  if(rows>0){
    cur_reader=data[1,1]
    if(cur_reader == pre_reader){	    
      val_tbl[hashmap[[as.character(data[1,2])]]] = data[1,3]
    }
    else{
      count=count+1
      idx = count%%mod_base
      if(idx!=0){
        out.data[idx,]=t(append(pre_reader,val_tbl))
      }
      else{
        out.data[mod_base,]=t(append(pre_reader,val_tbl))
        write.table(out.data,"D:/projects/Pivotal/feature_level2.csv",row.names=FALSE,col.names=FALSE,sep=",",quote=FALSE,append=TRUE)
        out.data=as.data.frame(t(append("",val_tbl)))
        gc()
      }
      
      val_tbl[]=0
      val_tbl[hashmap[[as.character(data[1,2])]]] = data[1,3]
      pre_reader=cur_reader
    }
    data=fetch(rs,1)
    rows=nrow(data)
  }
  else{
    count=count+1
    out.data[count,]=t(append(pre_reader,val_tbl))
    write.table(out.data,"D:/projects/Pivotal/feature_level2.csv",row.names=FALSE,col.names=FALSE,sep=",",quote=FALSE,append=TRUE)
    rows=-1
  }
}

dbClearResult(rs)
dbDisconnect(con_psql)
dbUnloadDriver(drv)