package cn.sheep.muifa.report

import cn.sheep.muifa.common.{AppUtils, C3p0Pools, ConfigHelper, ReportKPI}
import org.apache.commons.dbutils.QueryRunner
import org.apache.spark.sql.Row

/** 地域分布统计-core
  * author: old sheep
  * QQ: 64341393 
  * Created 2018/11/2
  */
object AreaAnalysisCore {

	def main(args: Array[String]): Unit = {

		val sc = AppUtils.createSparkContext("地域分布统计-core", master = "local[*]")

		// parquet -> sqlc
		val sqlc = AppUtils.createSQLContext(sc)
		val parquetFile = sqlc.read.parquet(ConfigHelper.parquetfilepath)

		/**
		  * 找分组的维度
		  * (K, V)
		  */
		parquetFile.map(row => {
			val pname = row.getAs[String]("provincename")
			val cname = row.getAs[String]("cityname")

			val (rawReq, effReq, adReq, rtbReq, succReq, adShow, adClick, cost, expense) = ReportKPI.offlineKPI(row)

			((pname, cname), List[Double](rawReq, effReq, adReq, rtbReq, succReq, adShow, adClick, cost, expense))
		}) /*.reduceByKey((a, b) => (
			a._1 + b._1,
			a._2 + b._2,
			a._3 + b._3,
			a._4 + b._4,
			a._5 + b._5,
			a._6 + b._6,
			a._7 + b._7,
			a._8 + b._8,
			a._9 + b._9,
		))*/ .reduceByKey((a, b) => {
			a.zip(b).map(tp => tp._1 + tp._2)
		}).map(tp => List[String](tp._1._1, tp._1._2) ::: tp._2.map(_.toString) ::: Nil).foreachPartition(iter => {

			val dataSource = C3p0Pools.getDataSource
			iter.foreach(tp => {
				val qr = new QueryRunner(dataSource)
				qr.update(
					"insert into muifa_rpt_area values(?,?,?,?,?,?,?,?,?,?,?)",
					tp: _*
				)
			})
		})


		sc.stop()
	}


}
