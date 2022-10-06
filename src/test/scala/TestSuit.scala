import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.scalatest.funsuite.AnyFunSuite

class TestSuit extends AnyFunSuite with SharedSparkContext with DataFrameSuiteBase{

  lazy val inputList = List(("panda", 9001.0, "test"), ("coffee", 9002.0, "test"))
  lazy  val INPUT_PATH = "/Users/mmt7989/Desktop/SPARK_TESTCASES/spark-scala-test//data/input/"

  test("dataframe should be equal to its self") {
    import spark.implicits._

    val actualDF = Seq(("adarsh", 1, true), ("mani", 2, false))
      .toDF("name", "id", "isChecked")

    /*val expectedDF = spark.read
      //.option("header", true)
      .csv(INPUT_PATH)*/

    //expectedDF.show(false)

    actualDF.coalesce(1).write.option("header", true).format("csv").save(INPUT_PATH)

    //assertDataFrameEquals(expectedDF, actualDF)
  }

}