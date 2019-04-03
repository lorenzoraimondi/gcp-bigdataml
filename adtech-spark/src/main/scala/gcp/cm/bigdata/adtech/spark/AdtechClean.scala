package gcp.cm.bigdata.adtech.spark

import org.apache.spark.{SparkConf, SparkContext}

object AdtechClean extends App {

  val conf = new SparkConf()
  conf.setMaster("local")
  conf.setAppName("Word Count")
  val sc = new SparkContext(conf)

  // Si può usare GS come file system distribuito nativo al posto di HDFS
  val impressions = sc.textFile("gs://abucket-for-codemotion/adtech/test.csv")

  val csv = impressions.map(line => line.split(","))

//  val cleaned = csv.map(rec => rec.take(2) ++ rec.drop(3).take(12))
  val cleaned = csv.map(rec => rec.take(2) ++ rec.slice(3, 15))

  val textfile = cleaned.map(rec => rec.mkString(","))

  // Si può usare GS come file system distribuito nativo al posto di HDFS
  textfile.saveAsTextFile("gs://abucket-for-codemotion/adtech/test_cleaned.csv")
//  textfile.coalesce(1, shuffle = true).saveAsTextFile("gs://abucket-for-codemotion/adtech/test_cleaned.csv")

}
