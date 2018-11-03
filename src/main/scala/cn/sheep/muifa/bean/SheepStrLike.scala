package cn.sheep.muifa.bean

/**
  * author: old sheep
  * QQ: 64341393 
  * Created 2018/10/31
  */
class SheepStrLike(val str: String) {

	def toIntX = try {
		str.toInt
	} catch {
		case _: Exception => 0
	}


	def toDoubleX = try {
		str.toDouble
	} catch {
		case _:Exception => 0d
	}

}

object SheepStrLike {

	implicit def str2SheeStrLike(str: String) = new SheepStrLike(str)

}
