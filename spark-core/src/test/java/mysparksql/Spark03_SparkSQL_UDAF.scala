package mysparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object Spark03_SparkSQL_UDAF {

    def main(args: Array[String]): Unit = {

        // TODO 创建SparkSQL的运行环境
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        val df = spark.read.json("datas/user.json")
        df.createOrReplaceTempView("user")

        spark.udf.register("ageAvg", new MyAvgUDAF())

        spark.sql("select ageAvg(age) from user").show


        // TODO 关闭环境
        spark.close()
    }
    /*
     自定义聚合函数类：计算年龄的平均值
     1. 继承UserDefinedAggregateFunction
     2. 重写方法(8)
     */
    class MyAvgUDAF extends UserDefinedAggregateFunction{
        override def inputSchema: StructType =
            new StructType(Array(
                StructField("age",LongType)
            ))


        override def bufferSchema: StructType =
            new StructType(Array(
                StructField("total",LongType),
                StructField("cnt",LongType)
            ))


        override def dataType: DataType = LongType

        override def deterministic: Boolean = true

        override def initialize(buffer: MutableAggregationBuffer): Unit = {
            buffer.update(0,0L)
            buffer.update(1,0L)
        }

        override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
            buffer.update(0,buffer.getLong(0)+input.getLong(0))
            buffer.update(1,buffer.getLong(1)+1)

        }

        override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

            buffer1.update(0,buffer1.getLong(0)+buffer2.getLong(0))
            buffer1.update(1,buffer1.getLong(1)+buffer2.getLong(1))
        }

        override def evaluate(buffer: Row): Any = {
            buffer.getLong(0)/buffer.getLong(1)
        }
    }
}
