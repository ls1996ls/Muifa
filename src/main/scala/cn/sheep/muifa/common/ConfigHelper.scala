package cn.sheep.muifa.common

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}

/** 解析resources下面的application.conf文件
  * author: old sheep
  * QQ: 64341393 
  * Created 2018/10/31
  */
object ConfigHelper {

	private val applicationConf = ConfigFactory.load()


	/**
	  * 解析原始日志所在路径
	  */
	val bzip2file = applicationConf.getString("muifa.bzip.inputpath")
	val parquetfilepath = applicationConf.getString("muifa.parquet.path")
	val appdictfilepath = applicationConf.getString("muifa.appdict.path")


	val driverClass = applicationConf.getString("muifa.mysql.driver")
	val url = applicationConf.getString("muifa.mysql.url")
	val username = applicationConf.getString("muifa.mysql.username")
	val password = applicationConf.getString("muifa.mysql.password")

	val redishost = applicationConf.getString("muifa.redis.host")



	val props = new Properties()
	props.setProperty("dirverClass", driverClass)
	props.setProperty("user", username)
	props.setProperty("password", password)

	/*val initProps = {
		props.setProperty("dirverClass", driverClass)
		props.setProperty("user", username)
		props.setProperty("password", password)
		props
	}*/



}
