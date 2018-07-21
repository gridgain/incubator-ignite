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

import java.time.LocalDateTime;
import java.util.List;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

class CustomValue {
    private String payload;
    private List<CustomNestedValue> nestedValueList;
    @QuerySqlField
    private LocalDateTime time;

    public CustomValue() { }

    public CustomValue(String payload, List<CustomNestedValue> nestedValueList, LocalDateTime time) {
        this.payload = payload;
        this.nestedValueList = nestedValueList;
        this.time = time;
    }

    public LocalDateTime getTime() {
        return time;
    }
}
