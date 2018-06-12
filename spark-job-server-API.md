#### 初始化SQLContext 上下文
curl -X POST "192.168.1.51:8090/contexts/pysql-context?context-factory=spark.jobserver.python.PythonSQLContextFactory"

#### 上传并创建spark-job 作业任务
curl --data-binary @dist/xxx.egg \
-H 'Content-Type: application/python-archive' 192.168.1.51:8090/binaries/{jobname}

此操作只需要操作一次就可以供以后重复调用。egg包上传后由spark-jobserver自动拷贝至/tmp/spark-jobserver/sqldao/data/ 目录下

#### 激活运行作业任务
curl -d 'input.strings = {"filename":xxx,"username":xxx,"resultDir":xxx}' \
"192.168.1.51:8090/jobs?appName={jobname}&classPath=algorithm.rec.SparkJob&context=pysql-context"

spark-job-server作业服务器部署地址为192.168.1.51   /home/S238/spark-jobserver/

三个接口实际封装在27服务器的PHP上供外部调用

spark-job-server github_url: https://github.com/spark-jobserver/spark-jobserver
                 python-api: https://github.com/spark-jobserver/spark-jobserver/blob/master/doc/python.md