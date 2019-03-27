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

package org.apache.ignite.console.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Utility methods.
 */
public class JsonUtils {
    /** */
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Default constructor.
     */
    private JsonUtils() {
        // No-op.
    }

    /**
     * @param v Value to serialize.
     * @return JSON value.
     * @throws IOException If failed.
     */
    public static String toJson(Object v) throws IOException {
        return MAPPER.writeValueAsString(v);
    }

    /**
     * @param json JSON.
     * @param cls Object class.
     * @return Deserialized object.
     * @throws IOException If deserialization failed.
     */
    public static <T> T fromJson(String json, Class<T> cls) throws IOException {
        return MAPPER.readValue(json, cls);
    }

    /**
     * @param src source of JSON.
     * @param cls Object class.
     * @return Deserialized object.
     * @throws IOException If deserialization failed.
     */
    public static <T> T fromJson(Reader src, Class<T> cls) throws IOException {
        return MAPPER.readValue(src, cls);
    }

    /**
     * @param json JSON.
     * @return Map with parameters.
     * @throws IOException If deserialization failed.
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> paramsFromJson(String json) throws IOException {
        return MAPPER.readValue(json, Map.class);
    }

    /**
     * @param errMsg Error message.
     * @param cause Exception.
     * @return JSON.
     * @throws IOException If serialization failed.
     */
    public static String errorToJson(String errMsg, Throwable cause) throws IOException {
        Map<String, String> data = new HashMap<>();

        String causeMsg = "";

        if (cause != null) {
            causeMsg = cause.getMessage();

            if (F.isEmpty(causeMsg))
                causeMsg = cause.getClass().getName();
        }

        data.put("message", errMsg + ": " + causeMsg);

        return MAPPER.writeValueAsString(data);
    }

    /**
     * @param map Map with parameters.
     * @param key Key name.
     * @param dflt Default value.
     * @return Value as string.
     */
    public static String getString(Map<String, Object> map, String key, String dflt) {
        Object res = map.get(key);

        if (res == null)
            return dflt;

        return res.toString();
    }

    /**
     * @param map Map with parameters.
     * @param key Key name.
     * @param dflt Default value.
     * @return Value as boolean.
     */
    public static boolean getBoolean(Map<String, Object> map, String key, boolean dflt) {
        Object res = map.get(key);

        if (res == null)
            return dflt;

        return (Boolean)res;
    }
}
