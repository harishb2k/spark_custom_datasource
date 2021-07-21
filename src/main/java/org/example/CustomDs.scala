package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object CustomDs extends App {

  // STEP 1 - Setup spark session for testing
  val spark = SparkSession.builder().master("local[*]").getOrCreate()

  val schema = StructType(StructField("id", IntegerType, true) :: Nil)

  // Step 1.1 - use my custom format. We will implement this format.
  spark.read.schema(schema).format("me").load.show

  // Step 2 - Create a relation provider
  // Spark look for a class of type "DataSourceRegister" which implements method shortName() and returns the format name
  // In this case MeRelationProvider is the class for us

  // Step 2.1 - Spark has to find this DataSourceRegister
  // Spark will use service loader, so you need to create a following file
  // ./src/main/resources/META-INF/services/org.apache.spark.sql.sources.DataSourceRegister
  // Put the full path of you MeRelationProvider class in this file
  // org.example.me.MeRelationProvider

  // Step 3 - Spark wants to create a relation (or table) to read your data
  // Your DataSourceRegister will need to implement "SchemaRelationProvider" interface to do this.
  // In out case we are giving a BaseRelation with PrunedFilteredScan
  //
  // Spark will get this object and will call buildScan() to read you data.
  // buildScan will give you a RDD which provides 2 things
  // 1. Information about underlying partitions
  // 2. compute() method which will give the real data from underlying system
}
