/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spark

import java.sql.Timestamp
import java.lang.{Long => JLong}

import javax.cache.CacheException
import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.spark.AbstractDataFrameSpec.TEST_CONFIG_FILE
import org.apache.ignite.spark.IgniteDataFrameSettings._
import org.apache.spark.sql.streaming.Trigger
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StreamingOptimizationsSpec extends StreamingSpec {
    private val IN_TBL_NAME = "INPUT"
    private val DFLT_CACHE_NAME = "default"

    private val input = Seq(
        Seq(1L, Timestamp.valueOf("2017-01-01 00:00:00"), "s1"),
        Seq(2L, Timestamp.valueOf("2017-01-02 00:00:00"), "s2"),
        Seq(3L, Timestamp.valueOf("2017-01-03 00:00:00"), "s3")
    )

    describe("Spark Streaming with Ignite Optimizations") {
        it("applies Ignite optimizations") {
            input.foreach(i => insertIntoTable(
                i.head.asInstanceOf[JLong],
                i(1).asInstanceOf[Timestamp],
                i(2).asInstanceOf[String]
            ))

            val stream = spark.read//Stream
                .format(FORMAT_IGNITE)
                .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
                .option(OPTION_TABLE, IN_TBL_NAME)
                .option(OPTION_OFFSET_POLICY, "incremental")
                .option(OPTION_OFFSET_FIELD, "id")
                .load()

            stream.createOrReplaceTempView("input")

            val sqlStream = spark.sql("SELECT input.ID, input.CH FROM input")

            //val qry = sqlStream.writeStream.format("console").trigger(Trigger.Once()).start()

            sqlStream.explain(true)

            //qry.awaitTermination()

            sqlStream
        }
    }

    override protected def beforeEach(): Unit = {
        super.beforeEach()
        createTable(IN_TBL_NAME)
    }

    override protected def afterEach(): Unit = {
        super.afterEach()
        dropTable(IN_TBL_NAME)
    }

    private def createTable(name: String): Unit =
        ignite.cache(DFLT_CACHE_NAME).query(new SqlFieldsQuery(
            s"CREATE TABLE $name(id LONG, ts TIMESTAMP, ch VARCHAR, PRIMARY KEY(id))"
        )).getAll

    private def dropTable(name: String): Unit = scala.util.control.Exception.ignoring(classOf[CacheException]) {
        ignite.cache(DFLT_CACHE_NAME).query(new SqlFieldsQuery(
            s"DROP TABLE $name"
        )).getAll
    }

    private def insertIntoTable(id: JLong, ts: Timestamp, ch: String): Unit =
        ignite.cache(DFLT_CACHE_NAME).query(new SqlFieldsQuery(
            s"INSERT INTO $IN_TBL_NAME(id, ts, ch) VALUES(?, ?, ?)"
        ).setArgs(id, ts, ch)).getAll
}