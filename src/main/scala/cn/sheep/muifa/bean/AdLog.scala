package cn.sheep.muifa.bean

/**
  * author: old sheep
  * QQ: 64341393 
  * Created 2018/10/31
  */
class AdLog(val sessionid: String,
			val advertisersid: Int,
			val adorderid: Int,
			val adcreativeid: Int,
			val adplatformproviderid: Int,
			val sdkversion: String,
			val adplatformkey: String,
			val putinmodeltype: Int,
			val requestmode: Int,
			val adprice: Double,
			val adppprice: Double,
			val requestdate: String,
			val ip: String,
			val appid: String,
			val appname: String,
			val uuid: String,
			val device: String,
			val client: Int,
			val osversion: String,
			val density: String,
			val pw: Int,
			val ph: Int,
			val long: String,
			val lat: String,
			val provincename: String,
			val cityname: String,
			val ispid: Int,
			val ispname: String,
			val networkmannerid: Int,
			val networkmannername: String,
			val iseffective: Int,
			val isbilling: Int,
			val adspacetype: Int,
			val adspacetypename: String,
			val devicetype: Int,
			val processnode: Int,
			val apptype: Int,
			val district: String,
			val paymode: Int,
			val isbid: Int,
			val bidprice: Double,
			val winprice: Double,
			val iswin: Int,
			val cur: String,
			val rate: Double,
			val cnywinprice: Double,
			val imei: String,
			val mac: String,
			val idfa: String,
			val openudid: String,
			val androidid: String,
			val rtbprovince: String,
			val rtbcity: String,
			val rtbdistrict: String,
			val rtbstreet: String,
			val storeurl: String,
			val realip: String,
			val isqualityapp: Int,
			val bidfloor: Double,
			val aw: Int,
			val ah: Int,
			val imeimd5: String,
			val macmd5: String,
			val idfamd5: String,
			val openudidmd5: String,
			val androididmd5: String,
			val imeisha1: String,
			val macsha1: String,
			val idfasha1: String,
			val openudidsha1: String,
			val androididsha1: String,
			val uuidunknow: String,
			val userid: String,
			val iptype: Int,
			val initbidprice: Double,
			val adpayment: Double,
			val agentrate: Double,
			val lrate: Double,
			val adxrate: Double,
			val title: String,
			val keywords: String,
			val tagid: String,
			val callbackdate: String,
			val channelid: String,
			val mediatype: Int) extends Product with Serializable {

	// 访问类中的元素，通过角标的方式
	override def productElement(n: Int): Any = n match {
		case 0 => sessionid
		case 1 => advertisersid
		case 2 => adorderid
		case 3 => adcreativeid
		case 4 => adplatformproviderid
		case 5 => sdkversion
		case 6 => adplatformkey
		case 7 => putinmodeltype
		case 8 => requestmode
		case 9 => adprice
		case 10 => adppprice
		case 11 => requestdate
		case 12 => ip
		case 13 => appid
		case 14 => appname
		case 15 => uuid
		case 16 => device
		case 17 => client
		case 18 => osversion
		case 19 => density
		case 20 => pw
		case 21 => ph
		case 22 => long
		case 23 => lat
		case 24 => provincename
		case 25 => cityname
		case 26 => ispid
		case 27 => ispname
		case 28 => networkmannerid
		case 29 => networkmannername
		case 30 => iseffective
		case 31 => isbilling
		case 32 => adspacetype
		case 33 => adspacetypename
		case 34 => devicetype
		case 35 => processnode
		case 36 => apptype
		case 37 => district
		case 38 => paymode
		case 39 => isbid
		case 40 => bidprice
		case 41 => winprice
		case 42 => iswin
		case 43 => cur
		case 44 => rate
		case 45 => cnywinprice
		case 46 => imei
		case 47 => mac
		case 48 => idfa
		case 49 => openudid
		case 50 => androidid
		case 51 => rtbprovince
		case 52 => rtbcity
		case 53 => rtbdistrict
		case 54 => rtbstreet
		case 55 => storeurl
		case 56 => realip
		case 57 => isqualityapp
		case 58 => bidfloor
		case 59 => aw
		case 60 => ah
		case 61 => imeimd5
		case 62 => macmd5
		case 63 => idfamd5
		case 64 => openudidmd5
		case 65 => androididmd5
		case 66 => imeisha1
		case 67 => macsha1
		case 68 => idfasha1
		case 69 => openudidsha1
		case 70 => androididsha1
		case 71 => uuidunknow
		case 72 => userid
		case 73 => iptype
		case 74 => initbidprice
		case 75 => adpayment
		case 76 => agentrate
		case 77 => lrate
		case 78 => adxrate
		case 79 => title
		case 80 => keywords
		case 81 => tagid
		case 82 => callbackdate
		case 83 => channelid
		case 84 => mediatype
	}

	// 类中有多少个属性
	override def productArity: Int = 85

	// 传入的对象是否是某个类型
	override def canEqual(that: Any): Boolean = that.isInstanceOf[AdLog]
}


object AdLog {

	import cn.sheep.muifa.bean.SheepStrLike._

	/**
	  * sugar
	  * @param fields
	  * @return
	  */
	def apply(fields: Array[String]): AdLog = new AdLog(
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
	
}