package cn.sheep.muifa.etl

import cn.sheep.muifa.bean.AdLog
import cn.sheep.muifa.bean.SheepStrLike._
import cn.sheep.muifa.common.{AppUtils, ConfigHelper}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/** 将原始日志转成parquet文件
  * author: old sheep
  * QQ: 64341393 
  * Created 2018/10/31
  */
object Bzip2ParquetV2 {

	def main(args: Array[String]): Unit = {

		// 创建离线应用程序的入口
		val sc = AppUtils.createSparkContext("将原始日志转成parquet文件", master = "local[*]")

		// 读取数据
		val bzipdata: RDD[String] = sc.textFile(ConfigHelper.bzip2file)

		val bzipAdLog: RDD[AdLog] = bzipdata.map(_.split(",", -1)).filter(_.size >= 85).map(AdLog(_))


		val sqlc = AppUtils.createSQLContext(sc)

		// parquet <- write.parquet <- DataFrame【RDD[A <: Product]】 <- sqlContext
		val dataFrame = sqlc.createDataFrame(bzipAdLog)

		dataFrame.write.parquet(ConfigHelper.parquetfilepath)

		sc.stop()
	}

}
