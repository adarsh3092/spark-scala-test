import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.scalatest.funsuite.AnyFunSuite

final class TestSuit extends AnyFunSuite
                                with SharedSparkContext
                                with DataFrameSuiteBase {

  override implicit def reuseContextIfPossible: Boolean = true // reuse the sparkSession

  lazy val INPUT_PATH = "/Users/mmt7989/Desktop/SPARK_TESTCASES/spark-scala-test//data/input/"

  lazy val actualSeq = Seq(("adarsh", Option(1), Option(true)), ("mani", Option(2), Option(false)))

  lazy val ROOT_PATH = System.getProperty("project.dir")
  import spark.implicits._
  test("dataframe should be equal to its self") {


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

  test("Experiment data test ") {

    lazy val expectedDF  = Seq(
                   ("asdda", "zxdddd",  "qwertyu",   1l, false, true,  "uiddzsxs"),
                   ("ccxsc", "awsdeer", "asdfghj",   1l, false, true,  "uiddzsxs"),
                   ("wsxdd", "qawsdew", "zxcvbnm",   1l, false, true,  "uiddzsxs"),
                   ("werdf", "qwaserrg","sdfcxvb",   1l, false, true,  "uiddzsxs"),
                   ("qaswe", "asdwee",  "asdfxcv",   1l, false, true,  "uiddzsxs"),
                   ("qwesd", "cfgsdw",   "asdfvcx",  1l, true,  true,  "uiddzsxs"),
                   ("qwrty", "ssccdw",   "zxcvbnh",  1l, true,  true,  "uiddzsxs"),
                   ("xcvsd", "ssffsx",  "qwertyg",   1l, true,  true,  "uiddzsxs"))
                   .toDF("vtc", "exp_id", "var_id" , "version", "is_control", "qualified", "experience_lvl2")

   // df.coalesce(1).write.option("header", true).format("csv").save(INPUT_PATH+"experiment")

    //vtc:string, exp_id: string, var_id:string	,version:long ,is_control:boolean, qualified:boolean, experience_lvl2:string

    lazy val resultDF = spark.read.option("header", true).option("inferSchema", true).csv(INPUT_PATH+"experiment")

    assertTrue(expectedDF.count == resultDF.count)
    assertDataFrameEquals(expectedDF, resultDF)
  }

}