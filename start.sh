nohup ./lbsas -log=info -dbmoc=20 -dbmic=10 -dbaddr="tusung:tusung123@tcp(rdsrrmqumrrmqum.mysql.rds.aliyuncs.com:3306)/cargts" -dbcachesize=800000 -tcpaddr="0.0.0.0:8082" -httpaddr="0.0.0.0:8083" >log.txt 2>&1 &
