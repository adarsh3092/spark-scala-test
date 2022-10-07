import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.scalatest.funsuite.AnyFunSuite

final class TestSuit extends AnyFunSuite
                                with SharedSparkContext
                                with DataFrameSuiteBase {

  override implicit def reuseContextIfPossible: Boolean = true // reuse the sparkSession

  lazy val INPUT_PATH = "/Users/mmt7989/Desktop/SPARK_TESTCASES/spark-scala-test/data/input/"

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

  test("Test Exception class") {

    assertThrows[Exception](throw new Exception)

  }

  test(" Experiment data test ") {

    lazy val expectedDF  = Seq(
                   ("asdda", "zxdddd",  "qwertyu",   1l, false, true,  "uiddzsxs"),
                   ("ccxsc", "awsdeer", "asdfghj",   1l, false, true,  "adsdggfgg"),
                   ("wsxdd", "qawsdew", "zxcvbnm",   1l, false, true,  "asfghjkk"),
                   ("werdf", "qwaserrg","sdfcxvb",   1l, false, true,  "uiddzsxs"),
                   ("qaswe", "asdwee",  "asdfxcv",   1l, false, true,  "uiddzsxs"),
                   ("qwesd", "cfgsdw",   "asdfvcx",  1l, true,  true,  "qwsdfghjk"),
                   ("qwrty", "ssccdw",   "zxcvbnh",  1l, true,  true,  "qwefghjk"),
                   ("xcvsd", "ssffsx",  "qwertyg",   1l, true,  true,  "dfqwerer"))
                   .toDF("vtc", "exp_id", "var_id" , "version", "is_control", "qualified", "experience_lvl2")

   // df.coalesce(1).write.option("header", true).format("csv").save(INPUT_PATH+"experiment")

    //vtc:string, exp_id: string, var_id:string	,version:long ,is_control:boolean, qualified:boolean, experience_lvl2:string

    lazy val resultDF = spark.read.option("header", true).option("inferSchema", true).csv(INPUT_PATH+"experiment")

    assertTrue(expectedDF.count == resultDF.count)
    assertDataFrameDataEquals(resultDF, resultDF)
  }


  test(" Experiment data after join transformation ") {
    lazy val expectedDF  = Seq(
      ("asdda", "zxdddd",  "qwertyu",   1l, false, "asdrfgedv", "page_name_1",  "web"      ,200l, 199l, 80l, "mamami", "2022-10-07"),
      ("ccxsc", "awsdeer", "asdfghj",   1l, false, "asdrfgedv", "page_name_2",  "landing"  ,200l, 199l, 80l, "mamami", "2022-10-07" ),
      ("wsxdd", "qawsdew", "zxcvbnm",   1l, false, "asdrfgedv", "page_name_3",  "listing"  ,200l, 199l, 80l, "mamami", "2022-10-07" ),
      ("werdf", "qwaserrg","sdfcxvb",   1l, false, "asdrfgedv", "page_name_4",  "payment"  ,200l, 199l, 80l, "mamami", "2022-10-07" ),
      ("qaswe", "asdwee",  "asdfxcv",   1l, false, "asdrfgedv", "page_name_5",  "review"   ,200l, 199l, 80l, "mamami", "2022-10-07" ),
      ("qwesd", "cfgsdw",   "asdfvcx",  1l, true,  "asdrfgedv", "page_name_6",  "details"  ,200l, 199l, 80l, "mamami", "2022-10-07" ),
      ("qwrty", "ssccdw",   "zxcvbnh",  1l, true,  "asdrfgedv", "page_name_7",  "traveller",200l, 199l, 80l, "mamami", "2022-10-07" ))
      .toDF("vtc", "exp_id", "var_id", "version", "is_control", "experience_lvl2", "page_name"	,"ad_location",
          "impressions", "views", "clicks", "tenant", "partition_day")

    //expectedDF.coalesce(1).write.option("header", true).option("inferSchema", true).format("csv").save(INPUT_PATH+"experiment_tansformed")

    //vtc:string, exp_id: string, var_id:string	,version:long ,is_control:boolean, qualified:boolean, experience_lvl2:string

   lazy val resultDF = spark.read.option("header", true).option("inferSchema", true).csv(INPUT_PATH+"experiment_tansformed")

    assertTrue(expectedDF.count == resultDF.count)
    assertDataFrameDataEquals(resultDF, resultDF)
  }

}