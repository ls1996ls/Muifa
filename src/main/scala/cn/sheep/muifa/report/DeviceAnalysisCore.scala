package cn.sheep.muifa.report

import cn.sheep.muifa.common.{AppUtils, ConfigHelper, ReportKPI}

/** 设备相关的报表统计
  * author: old sheep
  * QQ: 64341393 
  * Created 2018/11/3
  */
object DeviceAnalysisCore {


	def main(args: Array[String]): Unit = {

		val sc = AppUtils.createSparkContext("设备相关的报表统计", master = "local[*]")

		// parquet -> sqlc
		val sqlc = AppUtils.createSQLContext(sc)
		val parquetFile = sqlc.read.parquet(ConfigHelper.parquetfilepath)


		// (K, V)
		val baseData = parquetFile.map(row => {
			val ispName = row.getAs[String]("ispname") // 运营商
			val network = row.getAs[String]("networkmannername") // 网络类型
			val client = row.getAs[Int]("client") match {
				case 1 => "安卓"
				case 2 => "IOS"
				case 3 => "微软"
				case _ => "未知"
			}// 操作系统


			// 计算指标
			val (rawReq, effReq, adReq, rtbReq, succReq, adShow, adClick, cost, expense) = ReportKPI.offlineKPI(row)

			(ispName, network, client, List[Double](rawReq, effReq, adReq, rtbReq, succReq, adShow, adClick, cost, expense))
		}).cache()


		baseData.map(tp => (tp._1, tp._4))
			.reduceByKey((list1, list2) => list1 zip list2 map (tp => tp._1 + tp._2))
			.saveAsTextFile("F:\\muifa\\device\\ispname")

		baseData.map(tp => (tp._2, tp._4))
			.reduceByKey((list1, list2) => list1 zip list2 map (tp => tp._1 + tp._2))
			.saveAsTextFile("F:\\muifa\\device\\network")

		baseData.map(tp => (tp._3, tp._4))
			.reduceByKey((list1, list2) => list1 zip list2 map (tp => tp._1 + tp._2))
			.saveAsTextFile("F:\\muifa\\device\\client")

		// 释放缓存
		baseData.unpersist(false)

		sc.stop()
	}

}
