import org.apache.spark.{SparkConf, SparkContext}
import org.json4s._
import org.json4s.JsonDSL._
import java.io._
import org.json4s.jackson.JsonMethods._



object ying_cheng_task2 {
  def main()(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task2").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //    sc.setLogLevel("ERROR")

    val s = System.currentTimeMillis()

    //    val br = new BufferedReader(new InputStreamReader(System.in))
    //
    //    val reviewFile_path_list = br.readLine().split(" ")

    //    val reviewFile_path = "/Users/irischeng/IdeaProjects/Assignment1/yelp_dataset/review.json"
    //    val reviewFile_path = reviewFile_path_list(0)

    //    val reviewFile_path = "/Users/irischeng/IdeaProjects/Assignment1/yelp_dataset/review.json"

    val reviewFile_path = args(0)
    val n_partition = args(2).toInt
    val outputFile_path=args(1)
    val reviewFile_default = sc.textFile(reviewFile_path)
    //    val reviewFile_customized = sc.textFile(reviewFile_path, reviewFile_path_list(3).toInt)
    val reviewFile_customized = sc.textFile(reviewFile_path, n_partition.toInt)



    val start_default = System.currentTimeMillis()
    var n_partition_default = reviewFile_default.partitions.length
    var n_items_default = reviewFile_default.mapPartitions(s => Iterator(s.size), true).collect()

    //    println(n_partition_default)
    var bus_reviews_list_default = reviewFile_default.map(s => s.split(",")).map(s => (s(2).split(":")(1).trim(), 1)).reduceByKey((a, b) => a + b)
    var top10_business_default = bus_reviews_list_default.sortByKey().map(s => s.swap).sortByKey(false).map(s => s.swap).take(10)
    //    println(top10_business_default)
    //    var n_items_default = bus_reviews_list_default.map(s => s._1).mapPartitions(s => Iterator(s.size), true).collect()
    //    println(n_items_default)
    val end_default = System.currentTimeMillis()
    var exe_time_default = ((end_default - start_default)/1000).toFloat
    //    println(exe_time_default)



    val start_customized = System.currentTimeMillis()
    var n_partition_customized = reviewFile_customized.partitions.length
    var n_items_customized = reviewFile_customized.mapPartitions(s => Iterator(s.size), true).collect()

    //    println(n_partition_customized)
    var bus_reviews_list_customized = reviewFile_customized.map(s => s.split(",")).map(s => (s(2).split(":")(1).trim(), 1)).reduceByKey((a, b) => a + b)
    var top10_business_customized = bus_reviews_list_customized.sortByKey().map(s => s.swap).sortByKey(false).map(s => s.swap).take(10)
    //    println(top10_business_customized)
    //    var n_items_customized = bus_reviews_list_customized.map(s => s._1).mapPartitions(s => Iterator(s.size), true).collect()
    //    println(n_items_customized)
    val end_customized = System.currentTimeMillis()
    var exe_time_customized = ((end_customized - start_customized)/1000).toFloat
    //    println(exe_time_customized)

    val json1 = ("n_partition"-> n_partition_default)~("n_items"-> n_items_default.toList)~("exe_time"-> exe_time_default)
    val json2 = ("n_partition"-> n_partition_customized)~("n_items"-> n_items_customized.toList)~("exe_time"-> exe_time_customized)
    val json = ("default"-> json1)~("customized"->json2)~("explanation"->"The one whose execution time is shorter reduces the shuffling time between transformation and action.")
    val jsons = compact(render(json))

    //    val writer = new PrintWriter(new File(reviewFile_path_list(2)))
    val writer = new PrintWriter(new File(outputFile_path))

    writer.write(jsons)
    writer.close()
    val e = System.currentTimeMillis()
    //    println(e-s)

    //
    //        for(i <- n_items_customized){
    //          println(i)
    //        }
  }
}
