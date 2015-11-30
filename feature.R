library("PivotalR")

cid_pivo = db.connect(host="172.25.197.164", user="gpadmin", password="gpadmin", dbname="blade_summary_test_db")
drv = dbDriver("PostgreSQL")
con_psql = dbConnect(drv,host="172.25.197.164",user="gpadmin",password="gpadmin",dbname="blade_summary_test_db")

cate_data = dbGetQuery(conn=con_psql, "select level1 from google_vertical_master_level group by level1 order by level1")
rows=nrow(cate_data)
col_txt=""
val_tbl=c()
for(i in 1:rows){
  if(i==1){
    col_txt=paste(col_txt,"X",as.character(cate_data[i,1]),sep="")
  }
  else{
    col_txt=paste(col_txt,paste("X",as.character(cate_data[i,1]),sep=""),sep=",")
  }
  val_tbl[as.character(cate_data[i,1])]=0
}

rs=dbSendQuery(conn = con_psql,"select reader_id, vertical_id, sum(total_ac) as access from google_ana_data_level1 group by reader_id, 
               vertical_id order by reader_id, vertical_id")
data=fetch(rs,1000)
rows=nrow(data)
{
if(rows>0){
  pre_reader=data[1,1]
  col_txt=""
  val_txt=""
  
  for(i in 1:rows){
    cur_reader=data[i,1]
    
    if(cur_reader == pre_reader){
      if(col_txt==""){
        col_txt = paste(col_txt,"X",as.character(data[i,2]),sep="")
        val_txt = paste(val_txt,as.character(data[i,3]),sep="")
      }
      else{
        col_txt = paste(col_txt,paste("X",as.character(data[i,2]),sep=""),sep=",")
        val_txt = paste(val_txt,as.character(data[i,3]),sep=",")
      }
    }
    else{
      q_txt = paste("insert into feature_level1 ","( reader_id,",col_txt,") ","values ('",pre_reader,"',",val_txt,")",sep="")
      db.q(q_txt,conn.id = cid_pivo)
      pre_reader=cur_reader
      col_txt = paste("","X",as.character(data[i,2]),sep="")
      val_txt = paste("",as.character(data[i,3]),sep="")
    }
  }
}
else{
    q_txt = paste("insert into feature_level1 ","( reader_id,",col_txt,") ","values ('",pre_reader,"',",val_txt,")",sep="")
    db.q(q_txt,conn.id = cid_pivo)
    col_txt=""
    val_txt=""
    q_txt=""
    dbClearResult(rs)
    dbDisconnect(con_psql)
    db.disconnect(cid_pivo)
    dbUnloadDriver(drv)
}
}

