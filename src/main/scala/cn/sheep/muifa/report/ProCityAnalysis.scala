package cn.sheep.muifa.report

import cn.sheep.muifa.common.{AppUtils, ConfigHelper}
import org.apache.spark.sql.SaveMode

/** 省市数据分布离线分析 - SQL
  * author: old sheep
  * QQ: 64341393 
  * Created 2018/11/2
  */
object ProCityAnalysis {

	def main(args: Array[String]): Unit = {

		val sc = AppUtils.createSparkContext("省市数据分布离线分析", master = "local[*]")

		// parquet -> sqlc
		val sqlc = AppUtils.createSQLContext(sc)
		val parquetFile = sqlc.read.parquet(ConfigHelper.parquetfilepath)
		parquetFile.registerTempTable("adlogs")

		// 按照省市进行分组，组内求一下记录条数
		val result = sqlc.sql(
			"""
			  |select
			  |provincename, cityname, count(1) cnts
			  |from adlogs
			  |group by provincename, cityname
			""".stripMargin)

		/**
		  * 将数据写到磁盘文件-json
		  */
		// coalesce 将分数数量合并(多变少，没有shuffle)
		// result.coalesce(4).write.json("F:\\muifa\\procity-json")

		/**
		  * 将数据写入到 mysql
		  */
		result.write/*.mode(SaveMode.Append)*/.jdbc(ConfigHelper.url, "muifa_rpt_procity",  ConfigHelper.props)

		sc.stop()
	}

}
