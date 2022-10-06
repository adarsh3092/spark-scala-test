import org.apache.spark.sql._
object HelloSpark {

  def main(args:Array[String]): Unit ={
    val spark = SparkSession.builder()
      .appName("HelloWorld")
      .master("local[3]")
      .getOrCreate()

    import spark.implicits._
    val df = Seq(("adarsh", "1"), ("sumi", "2")).toDF("name", "id")
    
    df.show(false)
    println("Hello Spark World!!")
  }

}
