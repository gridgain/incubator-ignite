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

package org.apache.ignite.configuration.sample.app;

import io.javalin.Javalin;

/**
 *
 */
public class ConfigurableApp {
    private static final String CONF_URL = "/management/v1/configuration";

    /**
     *
     * @param args
     */
    public static void main(String[] args) {
        Javalin app = Javalin.create().start(8080);

        app.get(CONF_URL, ctx -> ctx.result("{\n" +
            "  \"local\": {\n" +
            "    \"baseline\": {\n" +
            "      \"auto_adjust\": {\n" +
            "        \"enabled\": true,\n" +
            "        \"timeout\": 10000\n" +
            "      }\n" +
            "    }}"));

        app.post(CONF_URL, ctx -> {
            System.out.println("-->>-->> [" + Thread.currentThread().getName() + "] contentType " + ctx.contentType());
            System.out.println("-->>-->> [" + Thread.currentThread().getName() + "] content " + ctx.body());
        });
    }
}
