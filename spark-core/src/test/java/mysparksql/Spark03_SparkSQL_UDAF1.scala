package mysparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}

object Spark03_SparkSQL_UDAF1 {

    def main(args: Array[String]): Unit = {

        // TODO 创建SparkSQL的运行环境
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        val df = spark.read.json("datas/user.json")
        df.createOrReplaceTempView("user")

        spark.udf.register("ageAvg", functions.udaf(new MyAvgUDAF()))

        spark.sql("select ageAvg(age) from user").show


        // TODO 关闭环境
        spark.close()
    }
    /*
     自定义聚合函数类：计算年龄的平均值
     1. 继承org.apache.spark.sql.expressions.Aggregator, 定义泛型
         IN : 输入的数据类型 Long
         BUF : 缓冲区的数据类型 Buff
         OUT : 输出的数据类型 Long
     2. 重写方法(6)
     */
    case class Buff( var total:Long, var count:Long )
    class MyAvgUDAF extends Aggregator[Long, Buff, Long]{
        override def zero: Buff = {
            Buff(0L,0L)
        }

        override def reduce(buff: Buff, in: Long): Buff = {
            buff.total=buff.total+in
            buff.count=buff.count+1
            buff
        }

        override def merge(b1: Buff, b2: Buff): Buff = {
            b1.total=b1.total+b2.total
            b1.count=b1.count+b2.count
            b1
        }

        override def finish(res: Buff): Long = {
            res.total/res.count
        }

        override def bufferEncoder: Encoder[Buff] = Encoders.product

        override def outputEncoder: Encoder[Long] = Encoders.scalaLong
    }
}
