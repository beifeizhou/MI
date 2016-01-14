package Tag

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection._
import breeze.linalg._
import scala.math._

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.util.MLUtils

object MI {
  def main(args: Array[String]){
    val Conf = new SparkConf().setAppName("tag").setMaster("local")
    val sc = new SparkContext(Conf)
//    val path = "/Users/nali/Beifei/ximalaya2015/code_ximalaya/code_tag_MI_20151127/input/"    
    val path = "" //set path
    val albumPlayNum = sc.textFile(path+"albumPlayNum.csv") //last month data (20151003-20151103)
    .map(s => s.split(","))    
    val albumPlayAllNum = albumPlayNum.map(s => (s(0).toInt, s(2).toInt))    
    val N = albumPlayAllNum.map(s => s._2).reduce(_+_).toDouble    
    val pm = albumPlayAllNum.map(s => (s._1, s._2/N))
    val pmMap = pm.collect.toMap

    val albumTags = sc.textFile(path+"albumTag.txt")
    .map(s => s.split(","))
    val album2tagNum = albumTags.map(s => (s(0).toInt, s.tail.size.toDouble)).collect.toMap

    def ptmpm (x: Int) = {
      if (!album2tagNum.contains(x)){
        0.0
      }else{
        if (!pmMap.contains(x)){
          0.0
        }else{
          1/album2tagNum(x) * pmMap(x)
        }
      }
    }

    val tagAlbums = albumTags.map(s => s.tail.map(ss => (ss, s(0).toInt))).flatMap(s => s).groupBy(s => s._1).
    map(s => (s._1, s._2.toList.map(ss => ss._2)))
    val ptMap = tagAlbums.map(s => (s._1, s._2.map(ss => ptmpm(ss)).reduce(_+_))).collect.toMap    


    def miCal (x: (String, List[Int])) = {
      val pt = if (ptMap.contains(x._1)) ptMap(x._1) else 0.0
      val mivalue = x._2.map{s=>
        if (album2tagNum.contains(s) & (pt != 0)){
          val a = math.log(1/album2tagNum(s)/pt)
          val b = ptmpm(s)
          a * b
        }else{
          0.0
        }
      }.reduce(_+_)
      (x._1, mivalue)
    }

    val mi = tagAlbums.map(s => miCal(s)).sortBy(s => s._2)
    mi.saveAsTextFile(path+"MutualInfo")
    mi.filter(s => s._2 != 0.0).saveAsTextFile(path+"MutualInfoNonZero")
    tagAlbums.map(s => (s._1, s._2.size)).sortBy(s => s._2).saveAsTextFile(path+"tmp")
  }
}
