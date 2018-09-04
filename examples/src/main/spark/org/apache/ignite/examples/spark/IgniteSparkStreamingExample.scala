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

import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.client.IgniteClient
import org.apache.ignite.configuration.{CacheConfiguration, ClientConfiguration}
import org.apache.ignite.Ignition

object IgniteSparkStreamingExample {
    def main(args: Array[String]): Unit = {
        val ignite = Ignition.start("examples/config/example-ignite.xml")
        try {
            defineMarketData()
        }
        finally {
            ignite.close()
        }
    }

    private def defineMarketData(): Unit = withIgniteClient(igniteClient => {
        igniteClient.query(new SqlFieldsQuery(
            "CREATE TABLE quote(time TIMESTAMP, symbol VARCHAR, price DOUBLE, PRIMARY KEY(time))"
        )).getAll

//        val qry = new SqlFieldsQuery("INSERT INTO person (id, name, city_id) values (?, ?, ?)")
//
//        cache.query(qry.setArgs(1L.asInstanceOf[JLong], "John Doe", 3L.asInstanceOf[JLong])).getAll
    })

    private def addMarketData(symbol: String, price: Double): Unit = withIgniteClient(igniteClient => {
        
    })

    private def withIgniteClient(body: IgniteClient => Unit): Unit = {
        val client = Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1:10800"))
        try {
            body
        }
        finally {
            client.close()
        }
    }
}
