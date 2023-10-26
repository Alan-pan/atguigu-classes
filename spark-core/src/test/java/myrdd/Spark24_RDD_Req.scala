package myrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark24_RDD_Req {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 案例实操

        // 1. 获取原始数据：时间戳，省份，城市，用户，广告
        val dataRDD = sc.textFile("datas/agent.log")

//        统计出每一个省份每个广告被点击数量排行的 Top3
val value: RDD[((String, String), Int)] = dataRDD.map(line => {
  val spls: Array[String] = line.split(" ")
  //省份_广告,1
  ((spls(1), spls(4)), 1)
})

      val value1: RDD[(String, (String, Int))] = value.reduceByKey(_ + _).map(t => {
        (t._1._1, (t._1._2, t._2))
      })
       value1.groupByKey().mapValues(itr => {
        itr.toList.sortBy(tup => {
          tup._2
        })(Ordering.Int.reverse).take(3)
      }).foreach(println)

        sc.stop()

    }
}
