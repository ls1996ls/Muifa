package cn.sheep.muifa.utils

import cn.sheep.muifa.common.ConfigHelper
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/** jedis连接池
  * author: old sheep
  * QQ: 64341393 
  * Created 2018/11/3
  */
object Jpools {

	private val jedisPoolConfig = new JedisPoolConfig()
	jedisPoolConfig.setMaxTotal(1000)
	jedisPoolConfig.setMaxIdle(10)
	// todo 更多配置查询


	private val jedisPool = new JedisPool(jedisPoolConfig, ConfigHelper.redishost)


	/**
	  * 从池子中返回一个redis连接
	  * @return
	  */
	def getJedis = jedisPool.getResource

}
