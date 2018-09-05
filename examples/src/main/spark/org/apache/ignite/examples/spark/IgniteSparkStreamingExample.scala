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

package org.apache.ignite.examples.spark

import java.nio.file.Paths
import java.util.Calendar
import java.util.concurrent.Executors

import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.configuration.ClientConfiguration
import org.apache.ignite.spark.IgniteDataFrameSettings
import org.apache.ignite.{Ignite, Ignition}
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters
import scala.util.Random
import scala.util.control.NonFatal

object IgniteSparkStreamingExample {
    private val APP_NAME = IgniteSparkStreamingExample.getClass.getSimpleName
    private val IGNITE_CONFIG = "examples/config/example-ignite.xml"
    private val QUOTE_TABLE = "quote"
    private val RESULT_TABLE = "result"

    def main(args: Array[String]): Unit =
        withResource(Ignition.start(IGNITE_CONFIG))(ignite => {
            defineMarketData()

            withResource(SparkSession.builder.appName(APP_NAME).master("local").getOrCreate())(spark => {
                val quote = spark.readStream
                    .format(IgniteDataFrameSettings.FORMAT_IGNITE)
                    .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE, IGNITE_CONFIG)
                    .option(IgniteDataFrameSettings.OPTION_TABLE, QUOTE_TABLE)
                    .option(IgniteDataFrameSettings.OPTION_OFFSET_POLICY, "timestamp")
                    .option(IgniteDataFrameSettings.OPTION_OFFSET_FIELD, "time")
                    .load

                quote.createOrReplaceTempView("quote")

                val result = spark.sql("SELECT 'IBM' as symbol, AVG(price) as avgPrice FROM quote WHERE symbol='IBM'")

                val qry = result.writeStream
                    .outputMode("complete")
                    .format(IgniteDataFrameSettings.FORMAT_IGNITE)
                    .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE, IGNITE_CONFIG)
                    .option("checkpointLocation", Paths.get(System.getProperty("java.io.tmpdir"), APP_NAME).toString)
                    .option(IgniteDataFrameSettings.OPTION_TABLE, RESULT_TABLE)
                    .option(IgniteDataFrameSettings.OPTION_STREAMER_ALLOW_OVERWRITE, value = true)
                    .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "symbol")
                    .start

                exeSvc.submit(new Runnable {
                    override def run(): Unit = {
                        val DURATION_SEC = 10
                        val ITERATION_CNT = 30

                        for (_ <- 1 to ITERATION_CNT) {
                            addMarketData(ignite, "IBM", rnd.nextDouble() * 500)

                            Thread.sleep(1000 * DURATION_SEC / ITERATION_CNT)

                            val res = getResult(ignite).map(res => res.flatten)

                            println(s"Result on ${Calendar.getInstance().getTime}: $res")
                        }

                        qry.stop()
                    }
                })

                qry.awaitTermination()
                exeSvc.shutdown()
            })
        })

    private def defineMarketData(): Unit =
        withResource(Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1:10800")))(ignite =>
            ignite.query(new SqlFieldsQuery(
                s"CREATE TABLE $QUOTE_TABLE(time TIMESTAMP, symbol VARCHAR, price DOUBLE, PRIMARY KEY(time))"
            )).getAll
        )

    private def addMarketData(ignite: Ignite, symbol: String, price: Double): Unit = {
        ignite.cache(s"SQL_PUBLIC_${QUOTE_TABLE.toUpperCase}").query(new SqlFieldsQuery(
            s"INSERT INTO $QUOTE_TABLE(time, symbol, price) VALUES(CURRENT_TIMESTAMP(), ?, ?)"
        ).setArgs(symbol, price.asInstanceOf[AnyRef])).getAll
    }

    private def getResult(ignite: Ignite): Option[Seq[Seq[Any]]] = {
        Option.apply(ignite.cache(s"SQL_PUBLIC_${RESULT_TABLE.toUpperCase}"))
            .map(cache => cache.query(new SqlFieldsQuery(s"SELECT * FROM $RESULT_TABLE")).getAll)
            .map(res =>
                JavaConverters.asScalaIteratorConverter(res.iterator()).asScala.toSeq
                    .map(r => JavaConverters.asScalaIteratorConverter(r.iterator()).asScala.toSeq)
            )
    }

    private def withResource[Resource <: AutoCloseable, Result](r: Resource)(f: Resource => Result): Result = {
        val resource = r
        var err: Option[Throwable] = None
        try {
            f(resource)
        }
        catch {
            case NonFatal(ex) => err = Some(ex)
                throw ex
        }
        finally {
            err match {
                case Some(ex) =>
                    try {
                        resource.close()
                    }
                    catch {
                        case NonFatal(sup) => ex.addSuppressed(sup)
                    }
                case None => resource.close()
            }
        }
    }

    private val exeSvc = Executors.newScheduledThreadPool(4)
    private val rnd = Random
}
