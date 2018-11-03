package cn.sheep.muifa.etl

import cn.sheep.muifa.common.{AppUtils, ConfigHelper}

/** 读取parquet文件
  * author: old sheep
  * QQ: 64341393 
  * Created 2018/10/31
  */
object Sql4Parquet {


	def main(args: Array[String]): Unit = {

		val sc = AppUtils.createSparkContext("读取parquet文件", master = "local[*]")
		val sqlc = AppUtils.createSQLContext(sc)

		val dataFrame = sqlc.read.parquet(ConfigHelper.parquetfilepath)

		dataFrame.show()

		sc.stop()
	}
}
