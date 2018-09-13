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

import javax.cache.CacheException
import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.spark.AbstractDataFrameSpec.TEST_CONFIG_FILE
import org.apache.ignite.spark.IgniteDataFrameSettings._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StreamingOptimizations extends StreamingSpec {
    private val IN_TBL_NAME = "INPUT"
    private val DFLT_CACHE_NAME = "default"

    describe("Spark Streaming with Ignite Optimizations") {
        it("applies Ignite optimizations") {
            val stream = spark.readStream
                .format(FORMAT_IGNITE)
                .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
                .option(OPTION_TABLE, IN_TBL_NAME)
                .option(OPTION_OFFSET_POLICY, "incremental")
                .option(OPTION_OFFSET_FIELD, "id")
                .load()

            val df = spark.sql(s"SELECT in1.id, in1.ch, in2.id, in2.ch FROM $IN_TBL_NAME in1 JOIN in1 as in2 ON in1.ch = in2.ch")
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
}