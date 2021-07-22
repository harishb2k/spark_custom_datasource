package org.example.me

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{Partition, SparkContext, TaskContext}

// Part 1 - Spark service loader will load this class when it has to handle a format "me"
// DataSourceRegister - interface is created to do this job
class MeRelationProvider extends DataSourceRegister with SchemaRelationProvider {
  override def shortName(): String = "me"

  // The caller must pass the sqlContext and schema to be used for this relation
  override def createRelation(_sqlContext: SQLContext, parameters: Map[String, String], _schema: StructType): BaseRelation = {
    new BaseRelation with PrunedFilteredScan {

      // The sql context to use
      override def sqlContext: SQLContext = _sqlContext

      // Schema for this relation
      override def schema: StructType = _schema

      // Method of PrunedFilteredScan - Spark will call this to build a RDD from your relation
      override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
        new HarishRdd(_sqlContext.sparkContext)
      }
    }
  }
}

// This is a implementation of RDD. It does 2 things
// 1. Give list of partitions - these partitions can be used by Spark to distribute work to all jobs
//    For example, this RDD says I have 20 partitions in by data, so Spark may launch 20 jobs and give
//    partition to each job
//
// 2. The real data pulling work is done in "compute". This method got the partition to read e.g. a partition may
//    say read row from 0-100 and other partition will say read rows from 101-200
//    Now this compute will read data fro requested partition and will return the data back to the client as a iterator
class HarishRdd(sc: SparkContext) extends RDD[Row](sc, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = Iterator(
    // Here we are giving a hard coded row - you can read it from your own data source
    Row(10)
  )

  // Here we are saying that we have only 1 partition in underlying data. You can have your custom class of Partition
  // and you can split the data to be pulled.
  // Since this Partition class is passed in the compute method. You will be able to know how much data has to be pulled
  // for a singel job
  override protected def getPartitions: Array[Partition] = Array(
    new Partition {
      override def index: Int = 0
    },
    new Partition {
      override def index: Int = 1
    },
    new Partition {
      override def index: Int = 2
    },
    new Partition {
      override def index: Int = 3
    },
    new Partition {
      override def index: Int = 4
    },
    new Partition {
      override def index: Int = 5
    },
    new Partition {
      override def index: Int = 6
    },
    new Partition {
      override def index: Int = 7
    },
    new Partition {
      override def index: Int = 8
    },
    new Partition {
      override def index: Int = 9
    },
    new Partition {
      override def index: Int = 10
    },
    new Partition {
      override def index: Int = 11
    },
    new Partition {
      override def index: Int = 12
    },
    new Partition {
      override def index: Int = 13
    }
  )
}
