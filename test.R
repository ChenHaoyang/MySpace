library("PivotalR", lib.loc="~/R/win-library/3.2")

cid = db.connect(host="172.25.197.164", user="gpadmin", password="gpadmin", dbname="blade_summary_test_db")
db.q("select setseed(0.2143)")

km_res = list()

kmax = 40
tmax = 20
fn_min.value = 0
fn_min.index = 0

for(i in 2:kmax){
  km_tmp = list()
  
  for(j in 1:tmax){
    sql_txt = paste("select * from madlib.kmeans_random('pca_score_std','row_vec',",i,",'madlib.dist_norm2','madlib.avg',5000)",sep="")
    km_tmp[[j]] = db.q(sql_txt)
    if(j==1){
      fn_min.value=km_tmp[[1]]$objective_fn
      fn_min.index = 1
    }
    else{
      if(km_tmp[[j]]$objective_fn < fn_min.value){
        fn_min.value=km_tmp[[j]]$objective_fn
        fn_min.index = j
      }
    }
  }
  #sql_txt = paste("select * from madlib.kmeanspp('pca_score','row_vec',",i,",'madlib.dist_norm2','madlib.avg',5000)",sep="")
  #km_res[[i]] = db.q(sql_txt)
  km_res[[i]] = km_tmp[[fn_min.index]]
  km_tmp = NULL
  gc()
}
#################################################################################################################
tmp_txt= km_res[[6]]$centroids
tmp_txt = chartr("{","[",tmp_txt)
tmp_txt = chartr("}","]",tmp_txt)
sql_txt=paste("select (madlib.closest_column(array",tmp_txt,",row_vec)).column_id as cluster_id from pca_score_std order by row_id",sep="")
clu_id = db.q(sql_txt,nrows="all")
write.table(clu_id,file="D:/projects/Pivotal/cluster_level1_raw_6.txt")
#################################################################################################################
#C-AIC for K-Means
kmeansAICc = function(fit){
  n = 480641
  mk = length(strsplit(fit$centroids,",")[[1]])
  D = fit$objective_fn
  return(D + 2*mk*n/(n-mk-1))
}

kaicc=sapply(km_res[2:kmax],kmeansAICc)
plot(seq(2,kmax),kaicc,xlab="Number of clusters",ylab="C-AIC",pch=20,cex=2)

db.disconnect(cid)

vc=rep(0,10000)
mt=matrix(rep(0,10000),1,10000)

time = proc.time()
for(i in 1:1000000)
{
  vc[10000]=1
}
cat(proc.time()[3]-time[3])

out.data=as.data.frame(matrix(rep(0,100),5,20))
out.data=cbind(data.frame(rep("",5)),out.data)

data<-data.frame(rep("",10000),stringsAsFactors=FALSE)
data[1,1]="asas"
system.time(for(i in 1:10000) data[i,1] <- "row")

data_li<-vector("list",length=10000)
system.time(for(i in 1:10000) data_li[[i]] <- "row")

data.ma = matrix(rep(0,10000),1,10000)
system.time(for(i in 1:10000) data.ma[1,i] <- 1)

data.vc=rep(0,10000)
system.time(for(i in 1:10000) data.vc[i] <- 1)
