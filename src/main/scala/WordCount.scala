import org.apache.spark.{SparkContext,SparkConf}


object WordCount {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile("/Users/jacky/Codes/Scala/logs/***.log")
    println(textFile.count())
   // val counts = textFile.filter(temp => temp.contains("vd=root")).filter(temp => temp.contains("subtype=\"forward\""))
          //.map(line => (line,1)).reduceByKey(_+_)
    //counts.foreach(println)
    val dataRDD = textFile.map(line => line.split(" ")
            .map(temp => temp.substring(temp.indexOf("=") + 1, temp.length)).mkString(" "))

    dataRDD.foreach(println)
    //dataRDD.foreach(println)


//    val lines = Array("What makes life dreary is the want of motive", "Hello Spark", "Hello World")
//    val linesRDD = sc.parallelize(lines)
//    val words = linesRDD.flatMap(line => line.split(" "))
//    words.collect.foreach(println)

//    val data = Array(Tuple2("David", "Math"), Tuple2("David", "Music"),
//      Tuple2("Mary", "Math"), Tuple2("Mary", "Art"),
//      Tuple2("Allin", "Computer"))
//    val dataRDD = sc.parallelize(data)
//    val grouped = dataRDD.groupByKey()
//    grouped.collect.foreach(println)
  }
}
