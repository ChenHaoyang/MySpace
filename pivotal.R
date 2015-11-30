library("PivotalR", lib.loc="~/R/win-library/3.2")
library("RPostgreSQL", lib.loc="~/R/win-library/3.2")

cid = db.connect(host="172.25.197.164", user="gpadmin", password="gpadmin", dbname="blade_summary_test_db")
data=db.data.frame("iris")
fit = madlib.lm(Sepal.Length ~ .-Species,data=data)
##############################################################################################################
#feature table creation
##############################################################################################################
cate_list = db.q("select level1 from google_vertical_master_level group by level1 order by level1")
cate_list = db.q("select ma_ctgiii_id from ma_category order by ma_ctgiii_id",nrows="all")

str=""
for(i in 1:nrow(cate_list)){
  temp = paste("X",as.character(cate_list[i,1]),sep="")
  temp = paste(temp,"bigint",sep=" ")
  if(i==1){
    str = paste(str,temp," default (0)",sep="")
  }
  else{
    str = paste(str,paste(temp," default (0)",sep=""),sep=",")
  }
}
str = paste(str,"total bigint default (0)",sep=",")
table_create_sql = paste("Drop table feature_level1; create table feature_level1(reader_id text,",str,sep="")
table_create_sql = paste("Drop table if exists feature_microad; create table feature_microad(reader_id text,",str,sep="")
table_create_sql = paste(table_create_sql,") WITH (APPENDONLY=true, ORIENTATION=parquet, COMPRESSTYPE=snappy, OIDS=FALSE)",sep=" ")
rs = db.q(table_create_sql,conn.id=cid)
db.disconnect(conn.id=cid)
#--------------------------------RPostgreSQL begins-----------------------------------------------------------
drv = dbDriver("PostgreSQL")
con = dbConnect(drv,host="172.25.197.164",user="gpadmin",password="gpadmin",dbname="blade_summary_test_db")

dbSendQuery(conn = con, "truncate feature_microad")
rs= dbSendQuery(conn = con, "copy feature_microad from '/home/gpadmin/chen/feature_mad.csv' using delimiters ','")
rs=dbSendQuery(conn = con,"select * from iris")

rs=dbSendQuery(conn = con,"select reader_id, vertical_id, sum(total_ac) as access from google_ana_data_level1 group by reader_id, 
        vertical_id order by reader_id, vertical_id")
qury="select "
for(i in 1:658){
  temp=paste("sum(x",as.character(i),") as x",as.character(i),sep="")
  if(i<658){
    qury = paste(qury, temp,",",sep="")
  }
  else{
    qury = paste(qury, temp," from ma_feature_std",sep="")
  }
}
rs=dbSendQuery(conn=con,qury)
data=fetch(rs,1)
qury="Drop table if exists ma_feature_std_modified;create table ma_feature_std_modified  WITH (APPENDONLY=true, ORIENTATION=parquet, COMPRESSTYPE=snappy, OIDS=FALSE) as select reader_id, "
for(i in 1:658){
  if(data[1,i]!=0){
    temp=paste("x",as.character(i)," as x",as.character(i),sep="")
    qury = paste(qury, temp,",",sep="")
  }
}
qury = paste(substring(qury,1,nchar(qury)-1)," from ma_feature_std",sep="")
dbGetQuery(conn=con,qury)


data=fetch(rs,1)
pre_reader=data[1,1]
rows=nrow(data)
col_txt=""
val_txt=""

while(rows>=0){
  if(rows>0){
    cur_reader=data[1,1]
    if(col_txt==""){
      col_txt = paste(col_txt,"X",as.character(data[1,2]),sep="")
      val_txt = paste(val_txt,as.character(data[1,3]),sep="")
    }
    else{
      col_txt = paste(col_txt,paste("X",as.character(data[1,2]),sep=""),sep=",")
      val_txt = paste(val_txt,as.character(data[1,3]),sep=",")
    }
    
    if(cur_reader != pre_reader){
      q_txt = paste("insert into feature_level1 ","( reader_id",col_txt,") ","values (",pre_reader,",",val_txt,")",sep="")
      db.q(q_txt,conn.id = cid)
      col_txt=""
      val_txt=""
      pre_reader=cur_reader
    }
    else{
      data=fetch(rs,1)
      rows=nrow(data)
    }
  }
  else{
    q_txt = paste("insert into feature_level1 ","( reader_id",col_txt,") ","values (",pre_reader,",",val_txt,")",sep="")
    dbSendQuery(conn = con,q_txt)
    col_txt=""
    val_txt=""
    dbClearResult(rs)
    dbDisconnect(con)
    dbUnloadDriver(drv)
  }

    
}
#rs=dbSendQuery(con,"select * from feature_level1")

##############################################################################################################

rs = db.q(
  "Drop function overpaid(emp);
  Drop table emp;", conn.id=cid)
pt=proc.time()
rval = db.q(
  "select count(*) from
  (SELECT reader_id
  FROM google_audience_list_vertical
  group by reader_id) as t", conn.id=cid)
pt=proc.time()-pt
cat(pt)

rs = db.q(
  "create or replace function test_spi_prep(text) returns text as'
    sp=pg.spi.prepare(arg1,c(NAMEOID,NAMEOID));
    pg.spi.execp('sp',list('oid','text'));
  'language 'plr';
  select test_spi_prep(’select oid, typname from pg_type
where typname = $1 or typname = $2’);",conn.id=cid)

drv = dbDriver("PostgreSQL")
con = dbConnect(drv,host="172.25.197.164",user="gpadmin",password="gpadmin",dbname="blade_summary_test_db")
rs = dbSendQuery(con,"select level1 from google_vertical_master_level group by level1")
data = fetch(rs,n=-1)
dbClearResult(rs)

run_time = proc.time()
test=paste("Time Consumption: ",as.character(proc.time()[3]-run_time[3]),sep="")
cat(test)

install.packages("sendmailR",repos="http://cran.ism.ac.jp",dependencies = TRUE)
library("sendmailR")

sender <- "<chen_haoyang@microad.co.jp>"
recipients <- "<hust.chenhaoyang@gmail.com>"
sendmail(sender,recipients,"Subject of the email","Body of the email",
         control=list(smtpServer="ASPMX.L.GOOGLE.COM"))

