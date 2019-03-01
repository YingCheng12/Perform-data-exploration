import java.io._

import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._







object ying_cheng_task1 {
  def main()(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task1").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //     sc.setLogLevel("ERROR")
    val s = System.currentTimeMillis()
    //    val reviewFile = sc.textFile("/Users/irischeng/IdeaProjects/Assignment1/yelp_dataset/review.json")
    val reviewFile_path = args(0)
    val outputFile_path = args(1)
    val reviewFile = sc.textFile(reviewFile_path)

    //
    //
    def add(a: Float, b: Float): Float = {
      var sum: Float = 0
      sum = a + b
      return sum
    }


    var n_review = reviewFile.count()
    //        println(n_review)


    var n_review_2018 = reviewFile.filter(line => line.contains("\"date\":\"2018-")).count()
    //    println(n_review_2018)

    var userList_RDD = reviewFile.map(s => s.split(",")).map(s => s(1).split(":")(1)).map(s => s.trim())
    //    println(userList_RDD.collect()(2))
    var n_user = userList_RDD.distinct().count()
    println(n_user)


    var user_review_list = reviewFile.map(s => s.split(",")).map(s => (s(1).split(":")(1).stripPrefix("\""),1)).reduceByKey((a, b) => a + b)
    var top10_user = user_review_list.sortByKey().map(s => s.swap).sortByKey(false).map(s => s.swap).map(s=> s.toString()).take(10)
    ////    top10_user.getClass.getSimpleName
    //    println(top10_user)

    //
    var busList_RDD = reviewFile.map(s => s.split(",")).map(s =>s(2).split(":")(1).trim())
    var n_business = busList_RDD.distinct().count()
    //    println(n_business)
    //
    var bus_reviews_list = reviewFile.map(s => s.split(",")).map(s => (s(2).split(":")(1).stripPrefix("\""), 1)).reduceByKey((a, b) => a+b)
    var top10_business = bus_reviews_list.sortByKey().map(s=>s.swap).sortByKey(false).map(s=>s.swap).take(10)


    var top10_user_string = "["
    for(i <- top10_user){
      //          println(i)
      //          println(i.getClass.getSimpleName)
      //          println(i.toList())

      val n = i.toString()
      var temp_str = n.slice(1, n.length-1)
      var temp : String = ""
      temp =  "[\"" + temp_str + "],"
      top10_user_string = top10_user_string.concat(temp)
      //          println(o)

    }
    top10_user_string = top10_user_string.slice(0, top10_user_string.length-1)
    top10_user_string= top10_user_string.concat("]")
    //        println(top10_user_string)


    var top10_business_string = "["
    for(i <- top10_business){
      //          println(i)
      //          println(i.getClass.getSimpleName)
      //          println(i.toList())

      val n = i.toString()
      var temp_str = n.slice(1, n.length-1)
      var temp : String = ""
      temp =  "[\"" + temp_str + "],"
      top10_business_string = top10_business_string.concat(temp)
      //          println(o)

    }
    top10_business_string = top10_business_string.slice(0, top10_user_string.length-1)
    top10_business_string= top10_business_string.concat("]")

    //    println(top10_business_string)





    //    val json = ("n_review"-> n_review)~("n_review_2018"-> n_review_2018)~("n_user"-> n_user)~
    //          ("top10_user"-> top10_user_string)~("n_business"->n_business)
    //          ("top10_business"->top10_business_string)

    val str = "{\"n_review\":" + n_review.toString() + "," + "\"n_review_2018\":" + n_review_2018.toString() + "," +"\"n_user\":" +n_user.toString()+"," + "\"top10_user\":" + top10_user_string+"," + "\"n_business\":"+ n_business.toString()+","+"\"top10_business\":"+top10_business_string+"}"
    //    println(str)
    //        val json = ("top10_user"-> top10_user)
    //    val jsons = compact(render(json))
    //        println(jsons)
    //////
    val writer = new PrintWriter(new File(outputFile_path))
    //
    writer.write(str)
    writer.close()
    val e = System.currentTimeMillis()
    println(e-s)


  }

}

