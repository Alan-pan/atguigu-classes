package com.atguigu.bigdata.spark.core.myrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Date

object Spark06_RDD_Operator_Transform_Test {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - groupBy
        val rdd = sc.textFile("datas/apache.log")
        val gbk: RDD[(Int, Iterable[(Int, Int)])] = rdd.map(line => {
            val strings = line.split(" ")
            val dtStr = strings(3)
            val format = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
            val date: Date = format.parse(dtStr)
            (date.getHours, 1)
        }).groupBy(_._1)



        gbk.map{
            case ( hour, iter ) => {
                (hour, iter.size)
            }
        }.collect.foreach(println)


        sc.stop()

    }
}
