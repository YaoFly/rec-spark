import random

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import Row
import math


class SparkJob:
    def __init__(self):
        pass


    def validate(self, context, runtime, config):
        """
        This method is called by the job server to allow jobs to validate their
        input and reject invalid job requests.
        :param context: the context to be used for the job. Could be a
        SparkContext, SQLContext, HiveContext etc.
        May be reused across jobs.
        :param runtime: the JobEnvironment containing run time information
        pertaining to the job and context.
        :param config: the HOCON config object passed into the job request
        :return: either JobData, which is parsed from config, or a list of
        validation problems.
        """
        problems = []
        job_data = None
        if not isinstance(context, SQLContext):
            problems.append('Expected a SQL context')
        if config.get('input.data', None):
            job_data = config.get('input.data')
        else:
            problems.append('config input.data not found')
        if len(problems) == 0:
            return job_data
        else:
            return 'ERROR'



    def run_job(self, context, runtime, data):
            """
            Entry point for the execution of a job
            :param context: the context to be used for the job.
            SparkContext, SQLContext, HiveContext etc.  
            May be reused across jobs
            :param runtime: the JobEnvironment containing run time information
            pertaining to the job and context.
            :param data: the JobData returned by the validate method
            :return: the job result OR a list of ValidationProblem objects.
            """
            spark = SparkSession \
                .builder \
                .appName("Python Spark SQL basic example") \
                .config("spark.some.config.option", "some-value") \
                .getOrCreate()
            pic888_data = spark.read.csv(data['filename'], sep="\t", header="true")
            pic888_data = pic888_data.withColumn('picid', pic888_data.picid.cast('long'))
            pic888_data = pic888_data.withColumn('xzuid', pic888_data.xzuid.cast('long'))
            pic888_data = pic888_data.select('picid', 'xzuid')

            # pic888_data.write.format("com.databricks.spark.csv").option('header', 'true').save('/result.csv', mode="overwrite")
            pic888_data.printSchema()
            rdfirst = random.randint(0, 10000);
            rdsecond = random.randint(0, 10000);
            TempViewName = "tmpTable"+str(rdfirst)+str(rdsecond);
            pic888_data.createOrReplaceTempView(TempViewName)

            # spark.sql("show tables").show()
            # spark.sql("select count(picid) from "+TempViewName).show()
            repartitionNum = 800

            def art(listRows):
                tempList = []
                bpicid = ''
                num = 0
                for line in listRows:
                    if bpicid == line[0]:
                        num += 1
                    else:
                        num = 1

                    if num < 51:
                        # tempList.append(Row(picid1=line[0], picid2=line[1], scorem=line[2]))
                        tempList.append(str(line[0])+','+str(line[1])+','+str(line[2]))
                    bpicid = line[0]
                return tempList
            def foreachT(y, x):
                if isinstance(x, dict):
                    for i in x:
                        if i in y:
                            y[i] += x[i]
                        else:
                            y[i] = x[i]
                else:
                    if not isinstance(y, dict):
                        y = dict()
                    if x.picid not in y:
                        y[x.picid] = 0
                    y[x.picid] += 1
                return y
            def getC(x):
                userC = dict()
                for i in x:
                    for j in x:
                        if i == j:
                            continue
                        if i not in userC:
                            userC[i] = dict()
                        if j not in userC[i]:
                            userC[i][j] = 0
                        userC[i][j] += 1 / math.log(1 + len(x) * 1.0)
                result = []
                for i in userC:
                    for j in userC[i]:
                        result.append((str(i) + ' ' + str(j), userC[i][j]))
                return result
            def getW(x):
                picidA = x[0].split(' ')
                if int(picidA[0]) not in broad_itemsLikeNum.value or int(picidA[1]) not in broad_itemsLikeNum.value:
                    return Row(picid1=picidA[0], picid2=picidA[1], scorem= 0)
                return Row(picid1=picidA[0], picid2=picidA[1], scorem= (x[1] / math.sqrt(broad_itemsLikeNum.value[int(picidA[0])] * broad_itemsLikeNum.value[int(picidA[1])])))

            def g(x):
                print (x)

            ###获取数据源Start
            result = spark.sql('select xzuid,picid from '+TempViewName+' group by xzuid,picid order by xzuid desc, picid desc')
            ###获取数据源END
            # print result.first()
            ###获取每个物品被用户喜欢的数量Start
            itemsLikeNum = result.rdd.reduce(lambda y, x: foreachT(y, x))
            ###获取每个物品被用户喜欢的数量END
            sc = context._sc
            broad_itemsLikeNum = sc.broadcast(itemsLikeNum)

            result = result.rdd.map(lambda x: (x.xzuid, x.picid)).groupByKey()
            # print (result)
            # print result.first()
            # print (len(result.collect()))
            result = result.flatMapValues(lambda x: getC(x))
            # print (result)
            # print result.first()
            # print len(result.collect())
            # print (100)
            result = result.repartition(repartitionNum)
            result = result.values().reduceByKey(lambda x, y: x+y)
            # print (20)
            # result = spark.createDataFrame(result)
            # print result.getNumPartitions()
            # print (200)
            result = result.map(lambda x: getW(x))
            # print result.first()
            # print (10)
            # result = result.repartition(repartitionNum)
            # print result.getNumPartitions()
            result = spark.createDataFrame(result)
            result = result.select('picid1', 'picid2', 'scorem')
            result.printSchema()
            result = result.withColumn('picid1', result.picid1.cast('long'))
            result = result.withColumn('picid2', result.picid2.cast('long'))
            result = result.withColumn('scorem', result.scorem.cast('Double'))
            result.printSchema()
            result.createOrReplaceTempView(TempViewName+'_result')
            resultData = spark.sql("select picid1, picid2, scorem from "+TempViewName+"_result order by picid1 desc, scorem desc, picid2 desc")
            # print (40)
            resultData = resultData.rdd.mapPartitions(art)
            # print (31)
            # print resultData.getNumPartitions()
            # resultData = resultData.repartition(repartitionNum)
            # print resultData.getNumPartitions()

            # print resultData.saveAsTextFile("hdfs://master:9000/result_foot10")
            # print 300
            # exit()
            resultData = resultData.map(lambda x:(x.split(",")[0],x.split(",")[1]))
            # resultData = resultData.groupByKey().map(lambda x : (x[0], list(x[1])))
            resultData = resultData.reduceByKey(lambda x,y:x+","+y)
            # print (resultData.first())


            resultData = spark.createDataFrame(resultData)
            # print (300)
            resultData.write.csv('/result10/result/'+data['username']+'/'+data['resultDir'], mode='overwrite', header='true')
            # print (30)
            return "success"