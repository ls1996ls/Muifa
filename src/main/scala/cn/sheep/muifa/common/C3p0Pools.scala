package cn.sheep.muifa.common

import com.mchange.v2.c3p0.ComboPooledDataSource

/**
  * author: old sheep
  * QQ: 64341393 
  * Created 2018/11/2
  */
object C3p0Pools {

	private val dataSource = new ComboPooledDataSource()
	dataSource.setDriverClass(ConfigHelper.driverClass)
	dataSource.setJdbcUrl(ConfigHelper.url)
	dataSource.setUser(ConfigHelper.username)
	dataSource.setPassword(ConfigHelper.password)

	// todo 可以设置连接池的其他的参数
	dataSource.setMaxIdleTime(3000)


	// 对外公开一个可以回去池子的方法
	def getDataSource = dataSource


}
