package myrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Hotcategory02 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    val sourceData: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    //1.复用
    sourceData.cache()

    val toMap = sourceData.flatMap(datas => {
      val spl: Array[String] = datas.split("_")
      spl match {
        case _ if spl(6) != "-1" => List((spl(6), (1, 0, 0)))
        case _ if spl(8) != "null" =>
          val ids = spl(8).split(",")
          ids.map(id => (id, (0, 1, 0))).toList
        case _ if spl(10) != "null" =>
          val ids = spl(10).split(",")
          ids.map(id => (id, (0, 0, 1))).toList
        case _ => Nil
      }

    })

    val trans: RDD[(String, (Int, Int, Int))] = toMap
      .reduceByKey((it1, it2) => {
        (it1._1 + it2._1, it1._2 + it2._2, it1._3 + it2._3)
      })

    trans.sortBy(_._2,false).take(10).foreach(println)

    sc.stop()
  }

}
