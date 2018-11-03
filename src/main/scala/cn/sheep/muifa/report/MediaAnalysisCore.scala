package cn.sheep.muifa.report

import cn.sheep.muifa.common.{AppUtils, ConfigHelper, ReportKPI}
import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast

/**
  * author: old sheep
  * QQ: 64341393 
  * Created 2018/11/3
  */
object MediaAnalysisCore {

	def main(args: Array[String]): Unit = {

		val sc = AppUtils.createSparkContext("设备相关的报表统计", master = "local[*]")

		// 读字典文件
		val appdict = sc.textFile(ConfigHelper.appdictfilepath)
			.map(_.split("\t", -1))
			.filter(_.length >= 5)
			.map(fields => {
				(fields(4), fields(1))
			}).collectAsMap()

		// 广播字典数据
		val appdictbcast: Broadcast[collection.Map[String, String]] = sc.broadcast(appdict)

		// parquet -> sqlc
		val sqlc = AppUtils.createSQLContext(sc)
		val parquetFile = sqlc.read.parquet(ConfigHelper.parquetfilepath)

		// (K, V)
		parquetFile.map(row => {
			var appName = row.getAs[String]("appname")

			if (StringUtils.isEmpty(appName)) {
				val appId = row.getAs[String]("appid")
				if (StringUtils.isNotEmpty(appId)) {
					// 去字典文件中找
					appName = appdictbcast.value.getOrElse(appId, appId)
				} else {
					appName = "未知"
				}
			}

			// 计算指标
			val (rawReq, effReq, adReq, rtbReq, succReq, adShow, adClick, cost, expense) = ReportKPI.offlineKPI(row)

			(appName, List[Double](rawReq, effReq, adReq, rtbReq, succReq, adShow, adClick, cost, expense))
		})
			.reduceByKey((list1, list2) => list1 zip list2 map (tp => tp._1 + tp._2))
    		.map(tp => tp._1+"\t"+tp._2.mkString("\t"))
			.saveAsTextFile("f:\\muifa\\media")


		sc.stop()
	}

}
