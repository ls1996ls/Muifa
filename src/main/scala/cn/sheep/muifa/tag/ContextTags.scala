package cn.sheep.muifa.tag

import cn.sheep.muifa.common.{AppUtils, ConfigHelper}
import cn.sheep.muifa.utils.Jpools
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

import scala.collection.mutable

/**
  * 用户上下文特征(标签)数据提取
  * author: old sheep
  * QQ: 64341393 
  * Created 2018/11/5
  */
object ContextTags {


	def main(args: Array[String]): Unit = {

		// 伪装身份
		System.setProperty("HADOOP_USER_NAME", "root")

		val sc = AppUtils.createSparkContext("用户上下文特征(标签)数据提取", master = "local[*]")

		// 广播停用词库
		val stopwords = sc.textFile(ConfigHelper.stopwodfilepath).map(word => (word, null)).collectAsMap
		val stopwordsBroadCast = sc.broadcast(stopwords)

		// parquet -> sqlc
		val sqlc = AppUtils.createSQLContext(sc)
		val parquetFile = sqlc.read.parquet(ConfigHelper.parquetfilepath)


		parquetFile.filter(
			"""
			  |imei!="" or mac != "" or idfa != "" or androidid !="" or openudid !="" or
			  |imeimd5!="" or macmd5 != "" or idfamd5 != "" or androididmd5 !="" or openudidmd5 !="" or
			  |imeisha1!="" or macsha1 != "" or idfasha1 != "" or androididsha1 !="" or openudidsha1 !=""
			""".stripMargin).mapPartitions(part => {

			val jedis = Jpools.getJedis

			val partResult = part.map(row => {
				val map = new mutable.HashMap[String, Int]()
				// 广告位类型
				val adSpaceType = row.getAs[Int]("adspacetype")
				if (adSpaceType > 0 && adSpaceType < 10) map += "LC0" + adSpaceType -> 1 else if (adSpaceType >= 10) map += "LC" + adSpaceType -> 1

				// app
				val appName = row.getAs[String]("appname")
				if (StringUtils.isNotEmpty(appName)) map += "APP" + appName -> 1
				else {
					val appId = row.getAs[String]("appid")
					if (StringUtils.isNotEmpty(appId)) {
						val appNewName = jedis.hget("appdict", appId)
						if (StringUtils.isNotEmpty(appNewName)) map += "APP" + appNewName -> 1
						else map += "APP" + appId -> 1
					}
				}

				// 渠道
				val channelId = row.getAs[Int]("adplatformproviderid")
				if (channelId > 0) map += "CN" + channelId -> 1

				// 地域
				val province = row.getAs[String]("provincename")
				val city = row.getAs[String]("cityname")
				if (StringUtils.isNotEmpty(province)) map += "ZP" + province -> 1
				if (StringUtils.isNotEmpty(city)) map += "ZC" + city -> 1

				// 终端设备
				val os = row.getAs[Int]("client")
				os match {
					case 1 => map += "D0000100001" -> 1
					case 2 => map += "D0000100002" -> 1
					case 3 => map += "D0000100003" -> 1
					case _ => map += "D0000100004" -> 1
				}
				val network = row.getAs[String]("networkmannername")
				network.toUpperCase match {
					case "2G" => map += "D0000200001" -> 1
					case "3G" => map += "D0000200002" -> 1
					case "4G" => map += "D0000200003" -> 1
					case "WIFI" => map += "D0000200004" -> 1
					case _ => map += "D0000200005" -> 1
				}
				val ispname = row.getAs[String]("ispname")
				ispname match {
					case "移动" => map += "D0000300001" -> 1
					case "联通" => map += "D0000300002" -> 1
					case "电信" => map += "D0000300003" -> 1
					case _ => map += "D0000300004" -> 1
				}

				// 关键字
				val keywords = row.getAs[String]("keywords")
				keywords.split("[|]", -1)
					.filter(kw => kw.length >= 3
						&& kw.length <= 8
						&& !stopwordsBroadCast.value.contains(kw)).foreach(kw => map += "K" + kw -> 1)
				(getUserId(row), map.toList)
			})
			jedis.close()
			partResult
		})
			.reduceByKey((list1, list2) => (list1:::list2).groupBy(_._1).mapValues(_.map(_._2).sum).toList)
			.saveAsTextFile("F:/muifa/context")


		sc.stop()
	}


	def getUserId(row: Row) = {

		row match {
			case row if StringUtils.isNotEmpty(row.getAs[String]("imei")) 			=> "IM:"+row.getAs[String]("imei")
			case row if StringUtils.isNotEmpty(row.getAs[String]("mac")) 			=> "MC:"+row.getAs[String]("mac")
			case row if StringUtils.isNotEmpty(row.getAs[String]("idfa")) 			=> "ID:"+row.getAs[String]("idfa")
			case row if StringUtils.isNotEmpty(row.getAs[String]("androidid")) 		=> "AD:"+row.getAs[String]("androidid")
			case row if StringUtils.isNotEmpty(row.getAs[String]("openudid")) 		=> "OU:"+row.getAs[String]("openudid")
			case row if StringUtils.isNotEmpty(row.getAs[String]("imeimd5")) 		=> "IM5:"+row.getAs[String]("imeimd5")
			case row if StringUtils.isNotEmpty(row.getAs[String]("macmd5"))			=> "MC5:"+row.getAs[String]("macmd5")
			case row if StringUtils.isNotEmpty(row.getAs[String]("idfamd5")) 		=> "ID5:"+row.getAs[String]("idfamd5")
			case row if StringUtils.isNotEmpty(row.getAs[String]("androididmd5")) 	=> "AD5:"+row.getAs[String]("androididmd5")
			case row if StringUtils.isNotEmpty(row.getAs[String]("openudidmd5")) 	=> "OU5:"+row.getAs[String]("openudidmd5")
			case row if StringUtils.isNotEmpty(row.getAs[String]("imeisha1")) 		=> "IMS:"+row.getAs[String]("imeisha1")
			case row if StringUtils.isNotEmpty(row.getAs[String]("macsha1")) 		=> "MCS:"+row.getAs[String]("macsha1")
			case row if StringUtils.isNotEmpty(row.getAs[String]("idfasha1")) 		=> "IDS:"+row.getAs[String]("idfasha1")
			case row if StringUtils.isNotEmpty(row.getAs[String]("androididsha1")) 	=> "ADS:"+row.getAs[String]("androididsha1")
			case row if StringUtils.isNotEmpty(row.getAs[String]("openudidsha1")) 	=> "OUS:"+row.getAs[String]("openudidsha1")
		}

	}

}
