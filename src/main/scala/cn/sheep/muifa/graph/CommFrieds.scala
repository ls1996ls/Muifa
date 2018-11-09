package cn.sheep.muifa.graph

import cn.sheep.muifa.common.AppUtils
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

/**
  * author: old sheep
  * QQ: 64341393
  * Created 2018/11/5
  */
object CommFrieds {

	def main(args: Array[String]): Unit = {


		val sc = AppUtils.createSparkContext("好基友", master = "local[*]")


		// 构建点集合 RDD[(VertexId, VD)]
		val verteies: RDD[(VertexId, (String, Int))] = sc.makeRDD(Seq(
			(1L, ("小小", 99)),
			(2L, ("大大", 69)),
			(14L, ("牛魔王", 9)),
			(16L, ("小甜甜", 78)),
			(12L, ("星哥", 10)),
			(13L, ("潘金莲", 100))
		))

		// 构建边集合 RDD[Edge[ED]]
		val edges: RDD[Edge[String]] = sc.makeRDD(Seq(
			Edge(1L, 2L, "不知道"),
			Edge(2L, 14L, "不知道"),
			Edge(14L, 16L, "不知道"),

			Edge(13L, 12L, "相公"),
			Edge(12L, 13L, "媳妇")
		))

		// 构建图对象
		val graph = Graph(verteies, edges)

		// 调用连通图算法
		/**
		  * 找到图中可以连通的分支，然后取其顶点，此时会得到一组数据：
		  * 	每个分支之中的点，都会向分支之中最小的顶点进行组合，组合成一个元组（点的ID, 改点所在分支中最小的顶点ID）
		  */
		val cc = graph.connectedComponents().vertices

		verteies.join(cc).map{
			case (_, ((name, fv), commId)) => (commId, List((name, fv)))
		}.reduceByKey(_ ++ _).foreach(println)

		// 得到结果
		// groupByKey reduceByKey区别？
		// cc.map(_.swap).groupByKey().map(_._2.mkString(",")).foreach(println)
		// cc.map(tp => (tp._2, List(tp._1))).reduceByKey(_ ++ _).foreach(println)


		sc.stop()

	}

}
