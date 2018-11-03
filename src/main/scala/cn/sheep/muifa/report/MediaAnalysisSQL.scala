package cn.sheep.muifa.report

import cn.sheep.muifa.common.{AppUtils, ConfigHelper, ReportKPI}
import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast

/**
  * author: old sheep
  * QQ: 64341393 
  * Created 2018/11/3
  */
object MediaAnalysisSQL {

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

		// 注册表
		parquetFile.registerTempTable("adlogs")


		// 自定义udf
		sqlc.udf.register("obtainNewAppName", (appId: String, appName: String) => {
			if (StringUtils.isEmpty(appName)) {
				if (StringUtils.isNotEmpty(appId)) {
					appdictbcast.value.getOrElse(appId, appId)
				} else {
					"未知"
				}
			} else appName
		})

		sqlc.sql(
			"""
			  |select obtainNewAppName(appid, appname) as appname,
			  |sum(if(requestmode=1 and processnode>=1, 1, 0)) rawReq,
			  |sum(if(requestmode=1 and processnode>=2, 1, 0)) effReq,
			  |sum(if(requestmode=1 and processnode=3, 1, 0)) adReq,
			  |
			  |sum(if(iseffective=1 and isbilling=1 and isbid=1 and adorderid!=0, 1, 0)) rbtReq,
			  |sum(if(iseffective=1 and isbilling=1 and iswin=1, 1, 0)) succReq,
			  |
			  |sum(if(requestmode=2 and iseffective=1, 1, 0)) adShow,
			  |sum(if(requestmode=3 and iseffective=1, 1, 0)) adClick,
			  |
			  |sum(if(iseffective=1 and isbilling=1 and iswin=1, winprice/1000, 0)) expense,
			  |sum(if(iseffective=1 and isbilling=1 and iswin=1, adpayment/1000, 0)) cost
			  |from adlogs
			  |group by obtainNewAppName(appid, appname)
			""".stripMargin).show(50)


		sc.stop()
	}

}
