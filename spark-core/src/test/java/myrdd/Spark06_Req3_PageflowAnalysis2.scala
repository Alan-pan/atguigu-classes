package myrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_Req3_PageflowAnalysis2 {

    def main(args: Array[String]): Unit = {

        // TODO : Top10热门品类
        val sparConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
        val sc = new SparkContext(sparConf)

        val actionRDD = sc.textFile("datas/user_visit_action.txt")

        val actionDataRDD = actionRDD.map(
            action => {
                val datas = action.split("_")
                UserVisitAction(
                    datas(0),
                    datas(1).toLong,
                    datas(2),
                    datas(3).toLong,
                    datas(4),
                    datas(5),
                    datas(6).toLong,
                    datas(7).toLong,
                    datas(8),
                    datas(9),
                    datas(10),
                    datas(11),
                    datas(12).toLong
                )
            }
        )
        actionDataRDD.cache()
        //计算分母page_id,cnt
        //仅计算分母1-2,2-3,3-4,4-5,5-6,6-7 前缀为分母 因为不会出现7 所以filter去除7 init取前缀
        val comDen: List[Int] = List(1, 2, 3, 4, 5, 6, 7)
        val transDen: List[(Int, Int)] = comDen.zip(comDen.tail)

        val den: Map[Long, Long] = actionDataRDD.filter(user=>comDen.init.contains(user.page_id)).map(user => {
            (user.page_id, 1L)
        }).reduceByKey(_ + _).collect().toMap

        //session_id切分,action_time排序
        //(session_id,action,page_id)
        val map1 = actionDataRDD.map {
            case user => {
                (user.session_id, (user.action_time, user.page_id))
            }
        }
        //(session,[(action,page_id),()]
        val map2: RDD[(String, List[(String, Long)])] = map1.groupByKey().mapValues {
            case itr => {
                itr.toList.sortBy(_._1)
            }
        }

        val returnPageCnt: RDD[((Long, Long), Long)] = map2.flatMap(data => {
            val pageList: List[Long] = data._2.map(itr => {
                itr._2
            })
            val tuples: List[((Long, Long), Long)] = pageList.zip(pageList.tail).filter(transDen.contains(_)).map(t => {
                (t, 1L)
            })
            tuples
        }).reduceByKey(_ + _)

        returnPageCnt.foreach{
            case ((page1,page2), cnt) => {
                val pageDen: Long = den.getOrElse(page1, 0)
                println(page1+"=>"+page2+",转化率"+cnt.toDouble/pageDen)
            }
        }

        sc.stop()
    }

    //用户访问动作表
    case class UserVisitAction(
              date: String,//用户点击行为的日期
              user_id: Long,//用户的ID
              session_id: String,//Session的ID
              page_id: Long,//某个页面的ID
              action_time: String,//动作的时间点
              search_keyword: String,//用户搜索的关键词
              click_category_id: Long,//某一个商品品类的ID
              click_product_id: Long,//某一个商品的ID
              order_category_ids: String,//一次订单中所有品类的ID集合
              order_product_ids: String,//一次订单中所有商品的ID集合
              pay_category_ids: String,//一次支付中所有品类的ID集合
              pay_product_ids: String,//一次支付中所有商品的ID集合
              city_id: Long
      )//城市 id
}
