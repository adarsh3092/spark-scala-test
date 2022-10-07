# spark-scala-test 

----
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

//string
 //vtc:string,exp_id: string,
 var_id:string	,
 version:long ,
 is_control:boolean,
 qualified:boolean	
 experience_lvl2:string


schema->2
 vtc:string	exp_id:string	var_id:string	version:long	is_control:string	
 experience_lvl2:string	page_name:string	ad_location:string	impressions:long	views:long	clicks:long	tenant:string
 	partition_day:string
depency
java 1.8
scala 2.11
maven 3.8.5
spark :2.3.1


<dependency>
            <groupId>com.holdenkarau</groupId>
            <artifactId>spark-testing-base_2.11</artifactId>
            <version>2.4.5_0.14.0</version>
            <scope>test</scope>
        </dependency>
