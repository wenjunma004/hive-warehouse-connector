/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.spark.sql.hive.llap.streaming.examples

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object NcHiveStreamingExample {

  def main(args: Array[String]): Unit = {
    if (args.length != 3 && args.length != 5) {
      // scalastyle:off println
      System.err.println(s"Usage: NcHiveStreamingExample <socket host> <socket port>")
      System.err.println(s"Usage: NcHiveStreamingExample " +
        s"<socket host> <socket port> <database> <table>")
      // scalastyle:on println
      System.exit(1)
    }

    val host = args(0)
    val port = args(1)
    val metastoreUri = args(2)

    val sparkConf = new SparkConf()
      .set("spark.sql.streaming.checkpointLocation", "./checkpoint")
    val sparkSession = SparkSession.builder()
      .appName("NcHiveStreamingExample")
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    import sparkSession.implicits._

    val socket = sparkSession.readStream
      .format("socket")
      .options(Map("host" -> host, "port" -> port))
      .load()
      .as[String]

    val (dbName, tableName) = if (args.length == 5) {
      (args(3), args(4))
    } else {
      (sparkConf.get("spark.datasource.hive.warehouse.dbname"),
        sparkConf.get("spark.datasource.hive.warehouse.tablename"))
    }
    val writer =
      socket.map { s =>
        val x = s.split(",")
        Schema(x(0).toInt, x(1).toInt, x(2).toInt, x(3).toInt, x(4).toInt, x(5).toInt,
          x(6).toInt, x(7).toInt, x(8).toInt, x(9).toInt, x(10).toInt, x(11).toInt,
          x(12).toInt, x(13).toInt, x(14).toInt, x(15).toInt, x(16).toInt, x(17).toInt,
          x(18).toInt, x(19).toFloat, x(20).toFloat, x(21).toFloat, x(22).toFloat,
          x(23).toFloat, x(24).toFloat, x(25).toFloat, x(26).toFloat, x(27).toFloat,
          x(28).toFloat, x(29).toFloat, x(30).toFloat, x(31).toFloat, x(32).toFloat,
          x(33).toFloat, x(34))
      }
        .writeStream
        .format("com.hortonworks.spark.sql.hive.llap.streaming.HiveStreamingDataSource")
        .option("metastoreUri", metastoreUri)
        .option("database", dbName)
        .option("table", tableName)

    // before this, a new terminal that runs 'nc -l <port>' has to be started and
    // csv records for web_sales table has to be pasted so that spark streaming
    // can read the rows from nc and pass it on to hive data source
    val query = writer.start()
    query.awaitTermination()

    query.stop()
    sparkSession.stop()
  }
}