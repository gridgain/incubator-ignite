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

package org.apache.ignite.yardstick.streamer;

import java.math.BigDecimal;
import java.time.LocalDate;

class CustomNestedValue {
    private LocalDate date;
    private BigDecimal bigDecimal;
    private String nestedStr;
    private long long1;

    public CustomNestedValue() { }

    public CustomNestedValue(LocalDate date, BigDecimal bigDecimal, String nestedStr, long long1) {
        this.date = date;
        this.bigDecimal = bigDecimal;
        this.nestedStr = nestedStr;
        this.long1 = long1;
    }
}
