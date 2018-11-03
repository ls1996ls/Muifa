package cn.sheep.muifa.common

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author: old sheep
  * QQ: 64341393 
  * Created 2018/10/31
  */
object AppUtils {

	/**
	  * 创建一个sparkContext实例
	  * @param appName 程序名称
	  * @param conf	   sparkconf的参数
	  * @param master
	  * @return
	  */
	def createSparkContext(appName: String, conf: Map[String, String] = null, master: String = null) = {
		val sparkConf = new SparkConf()
			.setAppName(appName)
			.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

		if (master != null) sparkConf.setMaster(master)

		if (conf != null) {
			conf.foreach(kv => sparkConf.set(kv._1, kv._2))
		}
		new SparkContext(sparkConf)
	}

	/**
	  * 创建一个SqlContext实例
	  * @param sc
	  * @return
	  */
	def createSQLContext(sc: SparkContext) = {
		new SQLContext(sc)
	}


	/**
	  * 将字符串转成int
	  * @param str
	  * @return
	  */
	def toInt(str: String) = {
		try {
			str.toInt
		} catch {
			case _ => 0
		}
	}

	/**
	  * 将字符串转成int
	  * @param str
	  * @return
	  */
	def toDouble(str: String) = {
		try {
			str.toDouble
		} catch {
			case _ => 0d
		}
	}

	//------------------------------------------------------------------------------//


}
