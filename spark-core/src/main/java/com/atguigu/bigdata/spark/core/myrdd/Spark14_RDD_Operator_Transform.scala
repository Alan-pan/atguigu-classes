package com.atguigu.bigdata.spark.core.myrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark14_RDD_Operator_Transform {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - (Key - Value类型)
        val rdd = sc.makeRDD(List(1,2,3,4),2)

        val value: RDD[(Int, Int)] = rdd.map((_, 1))
        value.partitionBy(new HashPartitioner(2))




        sc.stop()

    }
}
