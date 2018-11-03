package cn.sheep.muifa.report

import cn.sheep.muifa.bean.RptProCity
import cn.sheep.muifa.common.{AppUtils, C3p0Pools, ConfigHelper}
import com.google.gson.Gson
import org.apache.commons.dbutils.QueryRunner
import org.apache.spark.rdd.RDD

/** 使用core方式统计省市数据分布
  * author: old sheep
  * QQ: 64341393 
  * Created 2018/11/2
  */
object ProCityAnalysisCore {

	def main(args: Array[String]): Unit = {

		val sc = AppUtils.createSparkContext("省市数据分布离线分析", master = "local[*]")

		// parquet -> sqlc
		val sqlc = AppUtils.createSQLContext(sc)
		val parquetFile = sqlc.read.parquet(ConfigHelper.parquetfilepath)

		//RDD[(K, V)]
		val result: RDD[((String, String), Int)] = parquetFile.map(row => {
			// 获取省，市
			val proname = row.getAs[String]("provincename")
			val cityname = row.getAs[String]("cityname")

			((proname, cityname), 1)
		}).reduceByKey(_ + _)

		/*import sqlc.implicits._
		val resultDF = result.map(tp => (tp._1._1, tp._1._2, tp._2)).toDF("provincename", "cityname", "cnts")
		resultDF.write.json()*/

		/**
		  * 将数据写入到磁盘文件-json
		  * 使用gson将case class转成json
		  */
		result.map(tp => {
			val gson = new Gson()
			gson.toJson(RptProCity(tp._1._1, tp._1._2, tp._2))
		}).saveAsTextFile("F:\\muifa\\procity-json")

		/**
		  * 将数据写入到mysql
		  */
		result.foreachPartition(iter => {
			// 连接
			val dataSource = C3p0Pools.getDataSource
			iter.foreach{
				case ((provinceName, cityName), counts) => {
					// 写入到mysql
					val queryRunner = new QueryRunner(dataSource)
					queryRunner.update("insert into muifa_rpt_procity values(?,?,?)", provinceName, cityName, counts.asInstanceOf[AnyRef])
				}
			}
		})

		sc.stop()
	}

}
