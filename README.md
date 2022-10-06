# spark-scala-test 

---
###How ?

-----
So you include com.holdenkarau.spark-testing-base [spark_version]_1.1.2 and extend one of the classes and write some simple tests instead. For example to include this in a project using Spark 3.1.1:

```        

"com.holdenkarau" %% "spark-testing-base" % "3.1.1_1.1.2" % "test"      

```

or
```
<dependency>
    <groupId>com.holdenkarau</groupId>
    <artifactId>spark-testing-base_2.12</artifactId>
    <version>3.1.1_1.1.2</version>
    <scope>test</scope>
</dependency>
