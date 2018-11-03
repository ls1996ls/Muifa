package cn.sheep.muifa.common

import org.apache.spark.sql.Row

/**
  * author: old sheep
  * QQ: 64341393 
  * Created 2018/11/3
  */
object ReportKPI {

	/**
	  * 离线kpi业务
	  * @param row
	  * @return
	  */
	def offlineKPI(row: Row) = {
		val reqMode = row.getAs[Int]("requestmode")
		val proNode = row.getAs[Int]("processnode") // ctrl+shift+u

		// 原始请求 有效请求  广告请求
		val rawReq = if (reqMode == 1 && proNode >= 1) 1 else 0
		val effReq = if (reqMode == 1 && proNode >= 2) 1 else 0
		val adReq = if (reqMode == 1 && proNode == 3) 1 else 0


		val efftive = row.getAs[Int]("iseffective")
		val isbill = row.getAs[Int]("isbilling")
		val isbid = row.getAs[Int]("isbid")
		val iswin = row.getAs[Int]("iswin")
		val orderId = row.getAs[Int]("adorderid")

		// 参与竞价  竞价成功
		val rtbReq = if (efftive == 1 && isbill == 1 && isbid == 1 && orderId != 0) 1 else 0
		val succReq = if (efftive == 1 && isbill == 1 && iswin == 1) 1 else 0

		// 展示   点击
		val adShow = if (reqMode == 2 && efftive == 1) 1 else 0
		val adClick = if (reqMode == 3 && efftive == 1) 1 else 0

		// 成本及消费
		val costExpense = if (efftive == 1 && isbill == 1 && iswin == 1) {
			val winPrice = row.getAs[Double]("winprice")
			val adpayment = row.getAs[Double]("adpayment")
			(winPrice / 1000d, adpayment / 1000d)
		} else (0d, 0d)
		(rawReq, effReq, adReq, rtbReq, succReq, adShow, adClick, costExpense._1, costExpense._2)
	}

}
