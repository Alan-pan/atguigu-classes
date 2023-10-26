package mysparksql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Spark01_SparkSQL_Basic {

    def main(args: Array[String]): Unit = {

        // TODO 创建SparkSQL的运行环境
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()



        // TODO 执行逻辑操作

        // TODO DataFrame
        val df: DataFrame = spark.read.json("datas/user.json")
//        df.show()

        //DataFrame=>SQL
//        df.createOrReplaceTempView("user")
//        spark.sql("select age,username from user").show()
//        spark.sql("select avg(age) from user").show()

        //DataFrame=>DSL
//        df.select("age","username").show()
        import spark.implicits._
        //$隐式转换
//        df.select($"age"+1).show()
//        df.select('age+1).show()

        //RDD=>DF
        val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 13), (2, "lisi", 14)))
        val df1: DataFrame = rdd.toDF("id", "name", "age")
        val rdd1: RDD[Row] = df1.rdd

        //DF=>DS
        val ds2: Dataset[User] = df1.as[User]
        val df2: DataFrame = ds2.toDF()

        //RDD=>DS
        val ds3: Dataset[User] = rdd.map { case (id, name, age) => {
          User(id, name, age)
        }
        }.toDS()

      val rdd3: RDD[User] = ds3.rdd

        // TODO 关闭环境
        spark.close()
    }
    case class User( id:Int, name:String, age:Int )
}
