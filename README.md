### How Spark reads data from underlying source

Let's take this spark line which is reading data from some custom format "me". We will explore how all this works i.e.
how can you implement your custom data source. <br>
For example you can implement a datasource which reads from Redis and integrate with Spark.

```scala
val schema = StructType(StructField("id", IntegerType, true) :: Nil)
spark.read.schema(schema).format("me").load.show
```

<br>

#### Step 1 - Register your DataSourceRegister

Anytime spark sees a format e.g. "me" in this case. It has to find a implementation of DataSourceRegister class which
can handle this format. <br><br>

Now we want Spark to understand our format "me". To do this, we need to register a class, and tell Spark that use
class ```MeRelationProvider``` when it sees a format "me". <br><br>

1. Refer to class ```MeRelationProvider extends DataSourceRegister```. The only thing we have to implement here is
   method ```shortName() = "me```. <br>
2. Register this to service loader by following file:

```
./src/main/resources/META-INF/services/org.apache.spark.sql.sources.DataSourceRegister

You your class in this file:
org.example.me.MeRelationProvider
```

Now Spark can find your class to handle format "me". However, Spark wants to see this format or data source as a
relation i.e. Spark want so treat you data as a table. How do it are given below:

#### Step 2 - Tell Spark how to read the underlying data as a Table

You have some data, and you want to show it as a table to spark. This is done by
implementing ```SchemaRelationProvider```
interface in your DataSourceRegister class.

```scala
class MeRelationProvider extends DataSourceRegister with SchemaRelationProvider {
  override def createRelation(_sqlContext: SQLContext, parameters: Map[String, String], _schema: StructType): BaseRelation = ???
}
```

Here we will just return a ```BaseRelation``` instance (see the full impl in MeRelationProvider class). This will tell
Spark that this is a like a table. <br>

By now Spark considers it as a table. However, Spark still does not know how to read the data and send it to one or more
nodes to do parallel work.

#### Step 3 - Read the data from underlying source

This is done by implementing interface ```PrunedFilteredScan``` in base relation. This will tell Spark that you can
provide select columns and where clauses to the relation. <br>

```scala
override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
  new HarishRdd(_sqlContext.sparkContext)
}

// A Rdd to pull your data
class HarishRdd(sc: SparkContext) extends RDD[Row](sc, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = Iterator(
    Row(10)
  )

  override protected def getPartitions: Array[Partition] = Array(
    {
      new Partition {
        override def index: Int = 0
      }
    }
  )
}
```

Now Spark can call buildScan() method on your table. It will allow Spark to find how many partitions can be created from
this data (to do work in parallel). In this example we are saying we have only 1 partition. Spark will create a Job and
will call method RDD.compute(partition). <br>

Now the real data read happens in the method ```compute(split: Partition, context: TaskContext)```. You can implement it
by reading from your source e.g. MySQL, redis, some file etc

### Output of this code

Run ``CustomDs`` class and you will see the output which you returned from ```HarishRdd```

```shell
21/07/21 12:54:28 INFO DAGScheduler: Job 0 finished: show at CustomDs.scala:12, took 0.319093 s
+---+
| id|
+---+
| 10|
+---+
```

### Links

You can see all in the following video.

```shell
https://www.youtube.com/watch?v=YKkgVEgn2JE&list=PLmSqyu_jtjoMmMKJQDZnCh5XMC3p_RUDU&index=9
https://www.youtube.com/watch?v=vfd83ELlMfc&list=PLmSqyu_jtjoMmMKJQDZnCh5XMC3p_RUDU&index=10
```


