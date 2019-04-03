package gcp.cm.bigdata.adtech.spark

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}

object AdtechClean extends App {

  val props = new Properties
//  props.load(AdtechClean.getClass.getResourceAsStream("/gcp.properties"))
  props.load(getClass.getResourceAsStream("/gcp.properties"))

  val bucketName = props.getProperty("gcp.bucket")
  val inFile = props.getProperty("spark.in-file")
  val outFile = props.getProperty("spark.out-file")

  val conf = new SparkConf()
  conf.setMaster("local")
  conf.setAppName("Word Count")
  val sc = new SparkContext(conf)

  // Si può usare GS come file system distribuito nativo al posto di HDFS
  val impressions = sc.textFile(s"gs://$bucketName/$inFile")

  val csv = impressions.map(line => line.split(","))

//  val cleaned = csv.map(rec => rec.take(2) ++ rec.drop(3).take(12))
  val cleaned = csv.map(rec => rec.take(2) ++ rec.slice(3, 15))

  val textfile = cleaned.map(rec => rec.mkString(","))

  // Si può usare GS come file system distribuito nativo al posto di HDFS
  textfile.saveAsTextFile(s"gs://$bucketName/$outFile")
//  textfile.coalesce(1, shuffle = true).saveAsTextFile(s"gs://$bucketName/$outFile")

}
