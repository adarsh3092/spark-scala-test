import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.scalatest.funsuite.AnyFunSuite

final class TestSuit extends AnyFunSuite
                                with SharedSparkContext
                                with DataFrameSuiteBase {

  override implicit def reuseContextIfPossible: Boolean = true // reuse the sparkSession

  lazy val INPUT_PATH = "/Users/mmt7989/Desktop/SPARK_TESTCASES/spark-scala-test//data/input/"

  lazy val actualSeq = Seq(("adarsh", Option(1), Option(true)), ("mani", Option(2), Option(false)))

  lazy val ROOT_PATH = System.getProperty("project.dir")

  test("dataframe should be equal to its self") {
    import spark.implicits._

    lazy val actualDF = actualSeq.toDF("name", "id", "isChecked")

    lazy val expectedDF = spark.read.option("header", true).option("inferSchema", true).csv(INPUT_PATH)

    //actualDF.coalesce(1).write.option("header", true).format("csv").save(INPUT_PATH)
    assertDataFrameDataEquals(expectedDF, actualDF) //To just compare only data
    assertTrue(expectedDF.count() >= 2)  // expected row number check
    assertDataFrameEquals(expectedDF, actualDF) // TO check dataType & nullbilty
  }

  test("Test exception") {

    assertThrows[Exception](throw new Exception)

  }
}