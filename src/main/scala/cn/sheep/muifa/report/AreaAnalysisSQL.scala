package cn.sheep.muifa.report

import cn.sheep.muifa.common.{AppUtils, ConfigHelper}

/**
  * author: old sheep
  * QQ: 64341393 
  * Created 2018/11/2
  */
object AreaAnalysisSQL {

	def main(args: Array[String]): Unit = {

		val sc = AppUtils.createSparkContext("省市数据分布离线分析", master = "local[*]")

		// parquet -> sqlc
		val sqlc = AppUtils.createSQLContext(sc)
		val parquetFile = sqlc.read.parquet(ConfigHelper.parquetfilepath)
		parquetFile.registerTempTable("adlogs")

		// 自定义udf函数
		// sqlc.udf.register("sheepfunc", (flag: Boolean, succ: Int, fail: Int) => if (flag) succ else fail)

		sqlc.sql(
			"""
			  |select provincename, cityname,
			  |sum(sheepfunc(requestmode=1 and processnode>=1, 1, 0)) rawReq,
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
			  |
			  |from adlogs
			  |group by provincename, cityname
			""".stripMargin).write.jdbc(ConfigHelper.url, "muifa_rpt_area",  ConfigHelper.props)

		sc.stop()
	}

}
