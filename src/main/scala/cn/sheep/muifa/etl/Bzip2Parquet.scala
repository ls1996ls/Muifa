package cn.sheep.muifa.etl

import cn.sheep.muifa.common.{AppUtils, ConfigHelper}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import cn.sheep.muifa.bean.SheepStrLike._
import org.apache.spark.sql.types._

/** 将原始日志转成parquet文件
  * author: old sheep
  * QQ: 64341393 
  * Created 2018/10/31
  */
object Bzip2Parquet {

	def main(args: Array[String]): Unit = {

		// 创建离线应用程序的入口
		val sc = AppUtils.createSparkContext("将原始日志转成parquet文件", master = "local[*]")

		// 读取数据
		val bzipdata: RDD[String] = sc.textFile(ConfigHelper.bzip2file)

		val bzipRow: RDD[Row] = bzipdata.map(_.split(",", -1)).filter(_.size >= 85).map(fields => {
			Row(
				fields(0),
				fields(1).toIntX,
				fields(2).toIntX,
				fields(3).toIntX,
				fields(4).toIntX,
				fields(5),
				fields(6),
				fields(7).toIntX,
				fields(8).toIntX,
				fields(9).toDoubleX,
				fields(10).toDoubleX,
				fields(11),
				fields(12),
				fields(13),
				fields(14),
				fields(15),
				fields(16),
				fields(17).toIntX,
				fields(18),
				fields(19),
				fields(20).toIntX,
				fields(21).toIntX,
				fields(22),
				fields(23),
				fields(24),
				fields(25),
				fields(26).toIntX,
				fields(27),
				fields(28).toIntX,
				fields(29),
				fields(30).toIntX,
				fields(31).toIntX,
				fields(32).toIntX,
				fields(33),
				fields(34).toIntX,
				fields(35).toIntX,
				fields(36).toIntX,
				fields(37),
				fields(38).toIntX,
				fields(39).toIntX,
				fields(40).toDoubleX,
				fields(41).toDoubleX,
				fields(42).toIntX,
				fields(43),
				fields(44).toDoubleX,
				fields(45).toDoubleX,
				fields(46),
				fields(47),
				fields(48),
				fields(49),
				fields(50),
				fields(51),
				fields(52),
				fields(53),
				fields(54),
				fields(55),
				fields(56),
				fields(57).toIntX,
				fields(58).toDoubleX,
				fields(59).toIntX,
				fields(60).toIntX,
				fields(61),
				fields(62),
				fields(63),
				fields(64),
				fields(65),
				fields(66),
				fields(67),
				fields(68),
				fields(69),
				fields(70),
				fields(71),
				fields(72),
				fields(73).toIntX,
				fields(74).toDoubleX,
				fields(75).toDoubleX,
				fields(76).toDoubleX,
				fields(77).toDoubleX,
				fields(78).toDoubleX,
				fields(79),
				fields(80),
				fields(81),
				fields(82),
				fields(83),
				fields(84).toIntX
			)
		})

		// 构建schema信息
		val schema = StructType(Seq(
			StructField("sessionid", StringType),
			StructField("advertisersid", IntegerType),
			StructField("adorderid", IntegerType),
			StructField("adcreativeid", IntegerType),
			StructField("adplatformproviderid", IntegerType),
			StructField("sdkversion", StringType),
			StructField("adplatformkey", StringType),
			StructField("putinmodeltype", IntegerType),
			StructField("requestmode", IntegerType),
			StructField("adprice", DoubleType),
			StructField("adppprice", DoubleType),
			StructField("requestdate", StringType),
			StructField("ip", StringType),
			StructField("appid", StringType),
			StructField("appname", StringType),
			StructField("uuid", StringType),
			StructField("device", StringType),
			StructField("client", IntegerType),
			StructField("osversion", StringType),
			StructField("density", StringType),
			StructField("pw", IntegerType),
			StructField("ph", IntegerType),
			StructField("long", StringType),
			StructField("lat", StringType),
			StructField("provincename", StringType),
			StructField("cityname", StringType),
			StructField("ispid", IntegerType),
			StructField("ispname", StringType),
			StructField("networkmannerid", IntegerType),
			StructField("networkmannername", StringType),
			StructField("iseffective", IntegerType),
			StructField("isbilling", IntegerType),
			StructField("adspacetype", IntegerType),
			StructField("adspacetypename", StringType),
			StructField("devicetype", IntegerType),
			StructField("processnode", IntegerType),
			StructField("apptype", IntegerType),
			StructField("district", StringType),
			StructField("paymode", IntegerType),
			StructField("isbid", IntegerType),
			StructField("bidprice", DoubleType),
			StructField("winprice", DoubleType),
			StructField("iswin", IntegerType),
			StructField("cur", StringType),
			StructField("rate", DoubleType),
			StructField("cnywinprice", DoubleType),
			StructField("imei", StringType),
			StructField("mac", StringType),
			StructField("idfa", StringType),
			StructField("openudid", StringType),
			StructField("androidid", StringType),
			StructField("rtbprovince", StringType),
			StructField("rtbcity", StringType),
			StructField("rtbdistrict", StringType),
			StructField("rtbstreet", StringType),
			StructField("storeurl", StringType),
			StructField("realip", StringType),
			StructField("isqualityapp", IntegerType),
			StructField("bidfloor", DoubleType),
			StructField("aw", IntegerType),
			StructField("ah", IntegerType),
			StructField("imeimd5", StringType),
			StructField("macmd5", StringType),
			StructField("idfamd5", StringType),
			StructField("openudidmd5", StringType),
			StructField("androididmd5", StringType),
			StructField("imeisha1", StringType),
			StructField("macsha1", StringType),
			StructField("idfasha1", StringType),
			StructField("openudidsha1", StringType),
			StructField("androididsha1", StringType),
			StructField("uuidunknow", StringType),
			StructField("userid", StringType),
			StructField("iptype", IntegerType),
			StructField("initbidprice", DoubleType),
			StructField("adpayment", DoubleType),
			StructField("agentrate", DoubleType),
			StructField("lrate", DoubleType),
			StructField("adxrate", DoubleType),
			StructField("title", StringType),
			StructField("keywords", StringType),
			StructField("tagid", StringType),
			StructField("callbackdate", StringType),
			StructField("channelid", StringType),
			StructField("mediatype", IntegerType)
		))


		val sqlc = AppUtils.createSQLContext(sc)

		// parquet <- write.parquet <- DataFrame[RDD[Row] + StructType] <- sqlContext
		val dataFrame = sqlc.createDataFrame(bzipRow, schema)

		dataFrame.write.parquet(ConfigHelper.parquetfilepath)

		sc.stop()
	}

}
