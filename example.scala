val lines = spark.read.textFile(...).rdd        //rdd 0
val data = lines.map(...).cache()               //rdd 1
var kPoints = data.take(K)
while(tempDist > convergeDist) {
    val closest = data.map(...)                 //rdd 2,5,8
    val pStats = closest.reduceByKey(...)       //rdd 3,6,9
    val newPoints = pStats.map(...).collect()   //rdd 4,7,10
    //compute tempDist and updata kPoints
    ...
}