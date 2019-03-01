import org.apache.spark.{SparkConf, SparkContext}
import org.json4s._
import org.json4s.JsonDSL._
import java.io._
import org.json4s.jackson.JsonMethods._



object ying_cheng_task3 {
  def main()(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task3").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //    sc.setLogLevel("ERROR")

    val s = System.currentTimeMillis()

    //    val br = new BufferedReader(new InputStreamReader(System.in))
    //
    //    val reviewFile_path_list = br.readLine().split(" ")

    //    val reviewFile_path = "/Users/irischeng/IdeaProjects/Assignment1/yelp_dataset/review.json"
    val reviewFile_path = args(0)
    val reviewFile = sc.textFile(reviewFile_path)
    //    val businessFile_path = "/Users/irischeng/PycharmProjects/Assignment1/yelp_dataset/business.json"
    val businessFile_path = args(1)
    val businessFile = sc.textFile(businessFile_path)


    var reviewFile_RDD = reviewFile.map(s => s.split(",")).map(s => (s(2).split(":")(1).trim(), s(3).split(":")(1).trim()))
    var businessFile_RDD = businessFile.map(s => s.split(",\"")).map(s => (s(0).split(":")(1).trim(), s(3).slice(7, (s(3).length() - 1)).trim()))
    //    var n = businessFile_RDD.take(2)
    //

    var join_results = reviewFile_RDD.join(businessFile_RDD)
    //    var n = join_results.take(2)



    var city_sum_count = join_results.map(s => (s._2._2, s._2._1)).aggregateByKey((0.0,0.0))((u,v) => (u._1+v.toFloat, u._2+1), (u1, u2) =>(u1._1+u2._1, u1._2+u2._2))
    //    var n = city_sum_count.take(2)
    var average = city_sum_count.map(s=> (s._1, (s._2._1/s._2._2)))
    //    var n = average.take(2)
    var sort_average = average.sortByKey().map(s => s.swap).sortByKey(false).map(s => s.swap)
    //    var n = sort_average.take(2)
    //    var n_list = sort_average.collect()

    val start_m1 = System.currentTimeMillis()
    println(sort_average.collect().slice(0, 10))
    val end_m1 = System.currentTimeMillis()
    val m1 = ((end_m1 - start_m1)/1000).toFloat
    //    println(m1)


    val start_m2 = System.currentTimeMillis()
    println(sort_average.take(10))
    val end_m2 = System.currentTimeMillis()
    val m2 = ((end_m2 - start_m2)/1000).toFloat
    //    println(m2)
    //
    ////
    //    for(i <- n_list){
    //      println(i)
    //    }

    val writer1 = new PrintWriter(new File(args(2)))
    writer1.write("city, stars")
    for (i<- sort_average.collect().slice(2,sort_average.collect().length-1)){
      writer1.write("\n"+i._1+","+i._2.toString)
      //      println("\n"+i._1+","+i._2.toString)
      //      println(i._1.getClass.getSimpleName)
      //      println(i._2.getClass.getSimpleName)
    }
    writer1.close()





    val json = ("m1"-> m1)~("m2"->m2)~("explanation"->"When using RDD's collect() method, the data is loaded to the driver's memory which is time-consuming. However, take() retrieves only 10 in this case.")
    val jsons = compact(render(json))

    val writer = new PrintWriter(new File(args(3)))

    writer.write(jsons)
    writer.close()

    val e = System.currentTimeMillis()
    println(e-s)

  }
}


