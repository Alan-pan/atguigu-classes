package myrdd
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Date

object Spark01_Hotcategory01 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    val sourceData: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    //1.复用
    sourceData.cache()

    val clickRDD: RDD[(String, Int)] = sourceData.filter(datas => {
      val spl: Array[String] = datas.split("_")
      spl(6) != "-1"
    }).map(datas => {
      val spl: Array[String] = datas.split("_")
      (spl(6), 1)
    })
    val clickCnt: RDD[(String, Int)] = clickRDD.reduceByKey(_ + _)

    val orderRDD: RDD[(String, Int)] = sourceData.filter(datas => {
      val spl: Array[String] = datas.split("_")
      spl(8) != "null"
    }).flatMap(datas => {
      val spl: Array[String] = datas.split("_")
      val arrs: Array[String] = spl(8).split(",")
      val tuples: Array[(String, Int)] = arrs.map((_, 1))
      tuples
    })
    val orderCnt: RDD[(String, Int)] = orderRDD.reduceByKey(_ + _)


    val payRDD: RDD[(String, Int)] = sourceData.filter(datas => {
      val spl: Array[String] = datas.split("_")
      spl(10) != "null"
    }).flatMap(datas => {
      val spl: Array[String] = datas.split("_")
      val arrs: Array[String] = spl(10).split(",")
      arrs.map((_, 1))
    })
    val payCnt: RDD[(String, Int)] = payRDD.reduceByKey(_ + _)

    //三转
    //(品类Id,(clickcnt,ordercnt,paycnt))

//    val cg: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clickCnt.cogroup(orderCnt, payCnt)
//    val trans: RDD[(String, (Int, Int, Int))] = cg.mapValues { case (a, b, c) => {
//      //创建默认值
//      var cdef = 0
//      var odef = 0
//      var pdef = 0
//      if (a.iterator.hasNext) {
//        cdef = a.iterator.next()
//      }
//
//      if (b.iterator.hasNext) {
//        odef = b.iterator.next()
//      }
//
//      if (c.iterator.hasNext) {
//        pdef = c.iterator.next()
//      }
//
//      (cdef, odef, pdef)
//    }
//    }

    //除了cogroup之外union大表然后reduceBykey
    val cmap: RDD[(String, (Int, Int, Int))] = clickCnt.mapValues(v => {
      (v, 0, 0)
    })

    var omap: RDD[(String, (Int, Int, Int))] = orderCnt.mapValues(v => {
      (0, v, 0)
    })

    val pmap: RDD[(String, (Int, Int, Int))] = payCnt.mapValues(v => {
      (0, 0, v)
    })
    val trans: RDD[(String, (Int, Int, Int))] = cmap.union(omap).union(pmap)
      .reduceByKey((it1, it2) => {
        (it1._1 + it2._1, it1._2 + it2._2, it1._3 + it2._3)
      })

    trans.sortBy(_._2,false).take(10).foreach(println)

    sc.stop()
  }

}
