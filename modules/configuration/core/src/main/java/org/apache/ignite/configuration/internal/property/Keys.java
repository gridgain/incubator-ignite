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

package org.apache.ignite.configuration.internal.property;

/**
 * TODO: Add interface description.
 *
 * @author @java.author
 * @version @java.version
 */
public interface Keys {

    String LOCAL = "local";
    String BASELINE = "baseline";
    String AUTO_ADJUST = "auto_adjust";
    String NODE = "node";
    String ENABLED = "enabled";
    String TIMEOUT = "timeout";
    String CONSISTENT_ID = "consistentId";
    String PORT = "port";
    String LOCAL_BASELINE = concat(LOCAL, BASELINE);
    String LOCAL_BASELINE_AUTO_ADJUST = concat(LOCAL, BASELINE, AUTO_ADJUST);
    String LOCAL_BASELINE_NODES = concat(LOCAL, BASELINE, NODE);
    String LOCAL_BASELINE_NODES_CONSISTENT_ID = concat(LOCAL, BASELINE, NODE, CONSISTENT_ID);
    String LOCAL_BASELINE_NODES_PORT = concat(LOCAL, BASELINE, NODE, PORT);
    String LOCAL_BASELINE_AUTO_ADJUST_ENABLED = concat(LOCAL, BASELINE, AUTO_ADJUST, ENABLED);
    String LOCAL_BASELINE_AUTO_ADJUST_TIMEOUT = concat(LOCAL, BASELINE, AUTO_ADJUST, TIMEOUT);

    public static String concat(String... names) {
        StringBuilder res = new StringBuilder();

        for (String s : names)
            if(!s.isEmpty())
            res.append(".").append(s);

        return res.substring(1);
    }
}
