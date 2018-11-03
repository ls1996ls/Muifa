package cn.sheep.muifa.report

import cn.sheep.muifa.common.{AppUtils, ConfigHelper, ReportKPI}
import cn.sheep.muifa.utils.Jpools
import org.apache.commons.lang.StringUtils

/**
  * author: old sheep
  * QQ: 64341393 
  * Created 2018/11/3
  */
object MediaAnalysisCoreRedis {

	def main(args: Array[String]): Unit = {

		val sc = AppUtils.createSparkContext("设备相关的报表统计", master = "local[*]")

		// parquet -> sqlc
		val sqlc = AppUtils.createSQLContext(sc)
		val parquetFile = sqlc.read.parquet(ConfigHelper.parquetfilepath)

		// (K, V)
		parquetFile.mapPartitions(partition => {
			val jedis = Jpools.getJedis
			val result = partition.map(row => {
				var appName = row.getAs[String]("appname")

				if (StringUtils.isEmpty(appName)) {
					val appId = row.getAs[String]("appid")
					if (StringUtils.isNotEmpty(appId)) {
						// 去字典文件中找
						appName = jedis.hget("appdict", appId) match {
							case v if StringUtils.isNotEmpty(v) => v
							case _ => appId
						}
					} else {
						appName = "未知"
					}
				}

				// 计算指标
				val (rawReq, effReq, adReq, rtbReq, succReq, adShow, adClick, cost, expense) = ReportKPI.offlineKPI(row)
				(appName, List[Double](rawReq, effReq, adReq, rtbReq, succReq, adShow, adClick, cost, expense))
			})
			jedis.close()
			result
		})
			.reduceByKey((list1, list2) => list1 zip list2 map (tp => tp._1 + tp._2))
			.map(tp => tp._1 + "\t" + tp._2.mkString("\t"))
			.saveAsTextFile("f:\\muifa\\media1")


		sc.stop()
	}

}
