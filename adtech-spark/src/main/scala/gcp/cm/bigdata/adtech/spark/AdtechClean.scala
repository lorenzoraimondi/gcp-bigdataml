package gcp.cm.bigdata.adtech.spark

import org.apache.spark.{SparkConf, SparkContext}

object AdtechClean extends App {

  val conf = new SparkConf()
  conf.setMaster("local")
  conf.setAppName("Word Count")
  val sc = new SparkContext(conf)

  val impressions = sc.textFile("gs://abucket-for-codemotion/adtech/test")

  val csv = impressions.map(line => line.split(","))

  val cleaned = csv.map(rec => rec.take(2) ++ rec.drop(3).take(12))

  val textfile = cleaned.map(rec => rec.mkString(","))

  textfile.saveAsTextFile("gs://abucket-for-codemotion/adtech/test_cleaned")
//  textfile.coalesce(1, shuffle = true).saveAsTextFile("gs://abucket-for-codemotion/adtech/test_cleaned")

}
