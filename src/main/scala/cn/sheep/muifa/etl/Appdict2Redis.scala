package cn.sheep.muifa.etl

import cn.sheep.muifa.common.{AppUtils, ConfigHelper}
import cn.sheep.muifa.utils.Jpools
import org.apache.commons.lang.StringUtils

/** 将字典数据提取存储到redis
  * author: old sheep
  * QQ: 64341393 
  * Created 2018/11/3
  */
object Appdict2Redis {

	def main(args: Array[String]): Unit = {

		val sc = AppUtils.createSparkContext("将字典数据提取存储到redis", master = "local[*]")

		// 过滤合法的数据
		val filtered = sc.textFile(ConfigHelper.appdictfilepath).map(_.split("\t", -1))
			.filter(_.length >= 5)
			.map(arr => (arr(4), arr(1)))

		filtered.foreachPartition(partition => {
			val jedis = Jpools.getJedis
			partition.foreach {
				case (appId, appName) if (StringUtils.isNotEmpty(appId)) => jedis.hset("appdict", appId, appName)
			}
			jedis.close()
		})

		sc.stop()
	}

}
