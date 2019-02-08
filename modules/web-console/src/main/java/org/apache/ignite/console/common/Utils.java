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
package org.apache.ignite.console.common;

import java.util.Collection;
import io.vertx.core.buffer.Buffer;
import org.apache.ignite.console.db.dto.DataObject;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Utilities.
 */
public class Utils {
    /** */
    private static final Buffer EMPTY_ARRAY = Buffer.buffer("[]");

    /**
     * Convert query data to JSON array.
     *
     * @param data Query data.
     * @return Buffer with JSON array.
     */
    public static Buffer toJsonArray(Collection<? extends DataObject> data) {
        if (F.isEmpty(data))
            return EMPTY_ARRAY;

        Buffer buf = Buffer.buffer(4096);

        buf.appendString("[");

        data.forEach(item -> {
            if (buf.length() > 1)
                buf.appendString(",");

            buf.appendString(item.json());
        });

        buf.appendString("]");

        return buf;
    }
}
