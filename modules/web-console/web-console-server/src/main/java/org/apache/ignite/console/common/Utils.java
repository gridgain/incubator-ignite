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

import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Utilities.
 */
public class Utils {
//    /** */
//    private static final JsonObject EMPTY_OBJ = new JsonObject();

//    /** */
//    private static final List<CharSequence> HTTP_CACHE_CONTROL = Arrays.asList(
//        HttpHeaderValues.NO_CACHE,
//        HttpHeaderValues.NO_STORE,
//        HttpHeaderValues.MUST_REVALIDATE);

    /**
     * @param cause Error.
     * @return Error message or exception class name.
     */
    public static String errorMessage(Throwable cause) {
        String msg = cause.getMessage();

        return F.isEmpty(msg) ? cause.getClass().getName() : msg;
    }

    /**
     * @param a First set.
     * @param b Second set.
     * @return Elements exists in a and not in b.
     */
    public static TreeSet<UUID> diff(TreeSet<UUID> a, TreeSet<UUID> b) {
        return a.stream().filter(item -> !b.contains(item)).collect(Collectors.toCollection(TreeSet::new));
    }

    /**
     * @param json JSON object.
     * @param key Key with IDs.
     * @return Set of IDs.
     */
    public static TreeSet<UUID> idsFromJson(Object json, String key) {
        TreeSet<UUID> res = new TreeSet<>();

//        JsonArray ids = json.getJsonArray(key);
//
//        if (ids != null) {
//            for (int i = 0; i < ids.size(); i++)
//                res.add(UUID.fromString(ids.getString(i)));
//        }

        return res;
    }

//    /**
//     * @param json JSON to travers.
//     * @param path Dot separated list of properties.
//     * @return Tuple with unwind JSON and key to extract from it.
//     */
//    private static T2<JsonObject, String> xpath(JsonObject json, String path) {
//        String[] keys = path.split("\\.");
//
//        for (int i = 0; i < keys.length - 1; i++)
//            json = json.getJsonObject(keys[i], EMPTY_OBJ);
//
//        String key = keys[keys.length - 1];
//
//        if (json.containsKey(key))
//            return new T2<>(json, key);
//
//        throw new IllegalStateException("Parameter not found: " + path);
//    }

//    /**
//     * @param json JSON object.
//     * @param path Dot separated list of properties.
//     * @param def Default value.
//     * @return the value or {@code def} if no entry present.
//     */
//    public static boolean boolParam(JsonObject json, String path, boolean def) {
//        T2<JsonObject, String> t = xpath(json, path);
//
//        return t.getKey().getBoolean(t.getValue(), def);
//    }
//
//    /**
//     * @param json JSON object.
//     * @param path Dot separated list of properties.
//     * @return the value or {@code def} if no entry present.
//     */
//    public static boolean boolParam(JsonObject json, String path) throws IllegalArgumentException {
//        T2<JsonObject, String> t = xpath(json, path);
//
//        Boolean val = t.getKey().getBoolean(t.getValue());
//
//        if (val == null)
//            throw new IllegalArgumentException(missingParameter(path));
//
//        return val;
//    }
//
//    /**
//     * @param json JSON object.
//     * @param path Dot separated list of properties.
//     * @return {@link UUID} for specified path.
//     */
//    public static UUID uuidParam(JsonObject json, String path) {
//        T2<JsonObject, String> t = xpath(json, path);
//
//        return UUID.fromString(t.getKey().getString(t.getValue()));
//    }
//
//    /**
//     * @param data Collection of DTO objects.
//     * @return JSON array.
//     */
//    public static JsonArray toJsonArray(Collection<? extends DataObject> data) {
//        return data.stream().reduce(new JsonArray(), (a, b) -> a.add(new JsonObject(b.json())), JsonArray::addAll);
//    }

//    /**
//     * @param path Path to JKS file.
//     * @param pwd Optional password.
//     * @return Java key store options or {@code null}.
//     * @throws FileNotFoundException if failed to resolve path to JKS.
//     */
//    @Nullable public static JksOptions jksOptions(String path, String pwd) throws FileNotFoundException {
//        if (F.isEmpty(path))
//            return null;
//
//        File file = U.resolveIgnitePath(path);
//
//        if (file == null)
//            throw new FileNotFoundException("Failed to resolve path: " + path);
//
//        JksOptions jks = new JksOptions().setPath(file.getPath());
//
//        if (!F.isEmpty(pwd))
//            jks.setPassword(pwd);
//
//        return jks;
//    }

//    /**
//     * @param req Request.
//     * @return Request origin.
//     */
//    public static String origin(HttpServerRequest req) {
//        String proto = req.getHeader("x-forwarded-proto");
//
//        if (F.isEmpty(proto))
//            proto = req.isSSL() ? "https" : "http";
//
//        String host = req.getHeader("x-forwarded-host");
//
//        if (F.isEmpty(host))
//            host = req.host();
//
//        return proto + "://" + host;
//    }

//    /**
//     * @param ctx Context.
//     * @param errCode Error code.
//     * @param errPrefix Error message prefix.
//     * @param e Error to send.
//     */
//    public static void sendError(RoutingContext ctx, int errCode, String errPrefix, Throwable e) {
//        String err = errorMessage(e);
//
//        if (!F.isEmpty(errPrefix))
//            err = errPrefix + ": " + err;
//
//        ctx
//            .response()
//            .setStatusCode(errCode)
//            .end(err);
//    }

//    /**
//     * @param ctx Context.
//     * @param data Data to send.
//     */
//    public static void sendResult(RoutingContext ctx, Object data) {
//        Buffer buf;
//
//        if (data instanceof JsonObject)
//            buf = ((JsonObject)data).toBuffer();
//        else if (data instanceof JsonArray)
//            buf = ((JsonArray)data).toBuffer();
//        else
//            buf = Buffer.buffer(String.valueOf(data));
//
//        ctx
//            .response()
//            .putHeader(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
//            .putHeader(HttpHeaderNames.CACHE_CONTROL, HTTP_CACHE_CONTROL)
//            .putHeader(HttpHeaderNames.PRAGMA, HttpHeaderValues.NO_CACHE)
//            .putHeader(HttpHeaderNames.EXPIRES, "0")
//            .setStatusCode(HTTP_OK)
//            .end(buf);
//    }

    /**
     * Return missing parameter error message.
     *
     * @param param Parameter name.
     * @return Missing parameter error message.
     */
    public static String missingParameter(String param) {
        return "Failed to find mandatory parameter in request: " + param;
    }

//    /**
//     * @param ctx Context.
//     * @param paramName Parameter name.
//     * @return Parameter value
//     */
//    public static String pathParam(RoutingContext ctx, String paramName) {
//        String param = ctx.request().getParam(paramName);
//
//        if (F.isEmpty(param))
//            throw new IllegalArgumentException(missingParameter(paramName));
//
//        return param;
//    }

//    /**
//     * @param ctx Context.
//     * @param status Status to send.
//     */
//    public static void sendStatus(RoutingContext ctx, int status) {
//        ctx.response().setStatusCode(status).end();
//    }
}
