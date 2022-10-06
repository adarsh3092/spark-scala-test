import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.scalatest.funsuite.AnyFunSuite

final class TestSuit extends AnyFunSuite with SharedSparkContext
                                with DataFrameSuiteBase {

  lazy val INPUT_PATH = "/Users/mmt7989/Desktop/SPARK_TESTCASES/spark-scala-test//data/input/"

  lazy val actualSeq = Seq(("adarsh", 1, true), ("mani", 2, false))

  lazy val ROOT_PATH =  System.getProperty("project.dir")

  test("dataframe should be equal to its self") {
    import spark.implicits._

    lazy val actualDF = actualSeq.toDF("name", "id", "isChecked")

    lazy val expectedDF = spark.read.option("header", true).csv(INPUT_PATH)

    //expectedDF.show(false)

    //actualDF.coalesce(1).write.option("header", true).format("csv").save(INPUT_PATH)

    assertDataFrameDataEquals(expectedDF, actualDF)  //To just compare only data

    // assertDataFrameEquals(expectedDF, actualDF) // TO check dataType & nullbilty
  }

}