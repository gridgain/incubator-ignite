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

package org.apache.ignite.console.agent.rest;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.slf4j.LoggerFactory;


import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_FAILED;

/**
 * API to translate REST requests to Ignite cluster.
 */
public class RestExecutor implements AutoCloseable {
    /** */
    private static final IgniteLogger log = new Slf4jLogger(LoggerFactory.getLogger(RestExecutor.class));

//    /** JSON object mapper. */
//    private static final ObjectMapper MAPPER = new GridJettyObjectMapper();

//    /** */
//    private final OkHttpClient httpClient;

//    /** Index of alive node URI. */
//    private Map<List<String>, Integer> startIdxs = U.newHashMap(2);

//    /**
//     * Default constructor.
//     */
//    public RestExecutor() {
//        Dispatcher dispatcher = new Dispatcher();
//
//        dispatcher.setMaxRequests(Integer.MAX_VALUE);
//        dispatcher.setMaxRequestsPerHost(Integer.MAX_VALUE);
//
//        OkHttpClient.Builder builder = new OkHttpClient.Builder()
//            .readTimeout(0, TimeUnit.MILLISECONDS)
//            .dispatcher(dispatcher);
//
//        // Workaround for use self-signed certificate
//        if (Boolean.getBoolean("trust.all")) {
//            try {
//                SSLContext ctx = SSLContext.getInstance("TLS");
//
//                // Create an SSLContext that uses our TrustManager
//                ctx.init(null, new TrustManager[] {trustManager()}, null);
//
//                builder.sslSocketFactory(ctx.getSocketFactory(), trustManager());
//
//                builder.hostnameVerifier((hostname, session) -> true);
//            } catch (Exception ignored) {
//                LT.warn(log, "Failed to initialize the Trust Manager for \"-Dtrust.all\" option to skip certificate validation.");
//            }
//        }
//
//        httpClient = builder.build();
//    }

    /**
     * Stop HTTP client.
     */
    @Override public void close() {
//        if (httpClient != null) {
//            httpClient.dispatcher().executorService().shutdown();
//
//            httpClient.dispatcher().cancelAll();
//        }
    }

//    /** */
//    private RestResult parseResponse(Response res) throws IOException {
//        if (res.isSuccessful()) {
//            RestResponseHolder holder = MAPPER.readValue(res.body().byteStream(), RestResponseHolder.class);
//
//            int status = holder.getSuccessStatus();
//
//            switch (status) {
//                case STATUS_SUCCESS:
//                    return RestResult.success(holder.getResponse(), holder.getSessionToken());
//
//                default:
//                    return RestResult.fail(status, holder.getError());
//            }
//        }
//
//        if (res.code() == 401)
//            return RestResult.fail(STATUS_AUTH_FAILED, "Failed to authenticate in cluster. " +
//                "Please check agent\'s login and password or node port.");
//
//        if (res.code() == 404)
//            return RestResult.fail(STATUS_FAILED, "Failed connect to cluster.");
//
//        return RestResult.fail(STATUS_FAILED, "Failed to execute REST command: " + res.message());
//    }

//    /** */
//    private RestResult sendRequest(String url, Map<String, Object> params, Map<String, Object> headers) throws IOException {
//        HttpUrl httpUrl = HttpUrl.parse(url);
//
//        HttpUrl.Builder urlBuilder = httpUrl.newBuilder()
//            .addPathSegment("ignite");
//
//        final Request.Builder reqBuilder = new Request.Builder();
//
//        if (headers != null) {
//            for (Map.Entry<String, Object> entry : headers.entrySet())
//                if (entry.getValue() != null)
//                    reqBuilder.addHeader(entry.getKey(), entry.getValue().toString());
//        }
//
//        FormBody.Builder bodyParams = new FormBody.Builder();
//
//        if (params != null) {
//            for (Map.Entry<String, Object> entry : params.entrySet()) {
//                if (entry.getValue() != null)
//                    bodyParams.add(entry.getKey(), entry.getValue().toString());
//            }
//        }
//
//        reqBuilder.url(urlBuilder.build())
//            .post(bodyParams.build());
//
//        try (Response resp = httpClient.newCall(reqBuilder.build()).execute()) {
//            return parseResponse(resp);
//        }
//    }

    /** */
    private Map<String, RestResult> cache = new ConcurrentHashMap<>();

    /** */
    private List<String> commands = Arrays.asList(
        "top",
        "currentState",
        "VisorBaselineViewTask",
        "VisorCacheNamesCollectorTask",
        "VisorGridGainCacheConfigurationCollectorTask",
        "VisorGridGainNodeConfigurationCollectorTask",
        "VisorGridGainNodeDataCollectorTask",
        "VisorServiceTask"
    );

    /** */
    private RestResult compute(String key) {
        try {
            String path = "C:/Temp/dump";

            String subPath = commands.stream()
                .filter(key::contains)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Dump not found for key: " + key));

            List<Path> dumps = Files.list(Paths.get(path, subPath)).collect(Collectors.toList());

            for (Path dump : dumps) {
                List<String> lines = Files.readAllLines(dump);

                String params = lines.get(1);
                String data = lines.get(2).substring(10);

                if ("top".equals(subPath)) {
                    StringBuilder sb = new StringBuilder(data);

                    while (true) {
                        int ix1 = sb.indexOf(",\"caches\":[{\"name\":");
                        int ix2 = sb.indexOf("]", ix1);

                        if (ix1 > 0 && ix2 > 0)
                            sb.delete(ix1, ix2 + 1);
                        else
                            break;
                    }

                    data = sb.toString();

                    return  RestResult.success(data, null);
                }
                if ("VisorGridGainNodeConfigurationCollectorTask".equals(subPath)) {
                    String nid = key.substring(44);

                    if (params.contains(nid))
                        return RestResult.success(data, null);
                }
                else if ("VisorGridGainCacheConfigurationCollectorTask".equals(subPath)) {
                    String cache = key.substring(45);

                    if (params.contains(cache))
                        return RestResult.success(data, null);
                }
                else if ("VisorGridGainNodeDataCollectorTask".equals(subPath)) {
                    String nids = key.substring(35);

                    if (params.contains(nids)) {
//                        log.info("Collected nodes metrics: " + key);

                        return RestResult.success(data, null);
                    }
                }
                else if (params.contains(subPath))
                    return  RestResult.success(data, null);
            }
        }
        catch (Exception e) {
            log.error("Failed to find dump: " + key, e);

            return RestResult.fail(STATUS_FAILED, e.getMessage());
        }

        log.info(key);

        return RestResult.fail(STATUS_FAILED, " Failed to find dump: " + key);
    }

    /** */
    private String p1(String key) {
        int ix = key.indexOf(", p2");

        return key.substring(3, ix);
    }

    /** */
    private String p6(String key) {
        int ix1 = key.indexOf(", p6");
        int ix2 = key.indexOf(';', ix1);

        return key.substring(ix1 + 4, ix2);
    }

    /** */
    private AtomicInteger cnt = new AtomicInteger(0);

    /** */
    public RestResult sendRequest(List<String> nodeURIs, Map<String, Object> params, Map<String, Object> headers, boolean internal) {
        String key = String.valueOf(params);

        int n = -1;

        if (key.contains("cmd=top"))
            key = "cmd=top";
        else if (key.contains("currentState"))
            key = "currentState";
        else if (key.contains("VisorBaselineViewTask"))
            key = "VisorBaselineViewTask";
        else if (key.contains("VisorGridGainNodeConfigurationCollectorTask"))
            key = "VisorGridGainNodeConfigurationCollectorTask" + p1(key);
        else if (key.contains("VisorCacheNamesCollectorTask"))
            key = "VisorCacheNamesCollectorTask";
        else if (key.contains("VisorGridGainCacheConfigurationCollectorTask"))
            key = "VisorGridGainCacheConfigurationCollectorTask" + p6(key);
        else if (key.contains("VisorGridGainNodeDataCollectorTask")) {
            n = cnt.incrementAndGet();

            log.info("Request: " + n);

            key = "VisorGridGainNodeDataCollectorTask" + p1(key);
        }
        else if (key.contains("VisorServiceTask"))
            key="VisorServiceTask";
        else
            log.info("Unknown key:" + key);

        RestResult cached = cache.computeIfAbsent(key, this::compute);

        RestResult res = new RestResult(cached.getStatus(), cached.getError(), cached.getData());

        if (n > 0)
            log.info("Response: " + n);

        return res;

//        long tm = System.currentTimeMillis();
//
//        SB dump = new SB(1024 * 100);
//
//        dump.a("Time: ").a(tm).a('\n');
//        dump.a("Params: ").a(params).a('\n');
//
//        boolean exe = params.containsKey("p2");
//
//        String cmd =  String.valueOf(params.get(exe ? "p2" : "cmd"));
//
//        int sz1 = 0;
//        int sz2 = 0;
//
//        try {
//            Integer startIdx = startIdxs.getOrDefault(nodeURIs, 0);
//
//            for (int i = 0;  i < nodeURIs.size(); i++) {
//                Integer currIdx = (startIdx + i) % nodeURIs.size();
//
//                String nodeUrl = nodeURIs.get(currIdx);
//
//                try {
//                    RestResult res = sendRequest(nodeUrl, params, headers);
//
//                    LT.info(log, "Connected to cluster [url=" + nodeUrl + "]");
//
//                    startIdxs.put(nodeURIs, currIdx);
//
//                    String data = res.getData();
//
//                    dump.a("Response: ").a(data);
//
//                    if (!F.isEmpty(data)) {
//                        sz1 = data.length();
//                        sz2 = sz1;
//
//                        if ("top".equals(cmd)) {
//                            StringBuilder sb = new StringBuilder(data);
//
//                            while (true) {
//                                int ix1 = sb.indexOf(",\"caches\":[{\"name\":");
//                                int ix2 = sb.indexOf("]", ix1);
//
//                                if (ix1 > 0 && ix2 > 0)
//                                    sb.delete(ix1, ix2 + 1);
//                                else
//                                    break;
//                            }
//
//                            sz2 = sb.length();
//
//                            if (sz2 < sz1)
//                                res.setData(sb.toString());
//                        }
//                    }
//
//                    return res;
//                }
//                catch (ConnectException ignored) {
//                    // No-op.
//                }
//            }
//
//            LT.warn(log, "Failed connect to cluster. " +
//                "Please ensure that nodes have [ignite-rest-http] module in classpath " +
//                "(was copied from libs/optional to libs folder).");
//
//            throw new ConnectException("Failed connect to cluster [urls=" + nodeURIs + ", parameters=" + params + "]");
//        }
//        finally {
//            long dur = System.currentTimeMillis() - tm;
//
//            log.info("Command executed [cmd=" + cmd +
//                ", internal=" + internal +
//                ", duration=" + dur +
//                ", sz1=" + sz1 + ", sz2=" + sz2 + "]");
//
//            if (!internal) {
//                dump.a('\n').a("Duration: ").a(dur).a('\n').a("Size: ").a(sz1).a('\n');
//
//                Path path = Paths.get("./dump/dump_" + tm + ".txt").toAbsolutePath().normalize();
//
//                Files.createDirectories(path.getParent());
//
//                Files.write(path, dump.toString().getBytes(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
//            }
//        }
    }

//    /**
//     * REST response holder Java bean.
//     */
//    private static class RestResponseHolder {
//        /** Success flag */
//        private int successStatus;
//
//        /** Error. */
//        private String err;
//
//        /** Response. */
//        private String res;
//
//        /** Session token string representation. */
//        private String sesTok;
//
//        /**
//         * @return {@code True} if this request was successful.
//         */
//        public int getSuccessStatus() {
//            return successStatus;
//        }
//
//        /**
//         * @param successStatus Whether request was successful.
//         */
//        public void setSuccessStatus(int successStatus) {
//            this.successStatus = successStatus;
//        }
//
//        /**
//         * @return Error.
//         */
//        public String getError() {
//            return err;
//        }
//
//        /**
//         * @param err Error.
//         */
//        public void setError(String err) {
//            this.err = err;
//        }
//
//        /**
//         * @return Response object.
//         */
//        public String getResponse() {
//            return res;
//        }
//
//        /**
//         * @param res Response object.
//         */
//        @JsonDeserialize(using = RawContentDeserializer.class)
//        public void setResponse(String res) {
//            this.res = res;
//        }
//
//        /**
//         * @return String representation of session token.
//         */
//        public String getSessionToken() {
//            return sesTok;
//        }
//
//        /**
//         * @param sesTokStr String representation of session token.
//         */
//        public void setSessionToken(String sesTokStr) {
//            this.sesTok = sesTokStr;
//        }
//    }

//    /**
//     * Raw content deserializer that will deserialize any data as string.
//     */
//    private static class RawContentDeserializer extends JsonDeserializer<String> {
//        /** */
//        private final JsonFactory factory = new JsonFactory();
//
//        /**
//         * @param tok Token to process.
//         * @param p Parser.
//         * @param gen Generator.
//         */
//        private void writeToken(JsonToken tok, JsonParser p, JsonGenerator gen) throws IOException {
//            switch (tok) {
//                case FIELD_NAME:
//                    gen.writeFieldName(p.getText());
//                    break;
//
//                case START_ARRAY:
//                    gen.writeStartArray();
//                    break;
//
//                case END_ARRAY:
//                    gen.writeEndArray();
//                    break;
//
//                case START_OBJECT:
//                    gen.writeStartObject();
//                    break;
//
//                case END_OBJECT:
//                    gen.writeEndObject();
//                    break;
//
//                case VALUE_NUMBER_INT:
//                    gen.writeNumber(p.getBigIntegerValue());
//                    break;
//
//                case VALUE_NUMBER_FLOAT:
//                    gen.writeNumber(p.getDecimalValue());
//                    break;
//
//                case VALUE_TRUE:
//                    gen.writeBoolean(true);
//                    break;
//
//                case VALUE_FALSE:
//                    gen.writeBoolean(false);
//                    break;
//
//                case VALUE_NULL:
//                    gen.writeNull();
//                    break;
//
//                default:
//                    gen.writeString(p.getText());
//            }
//        }
//
//        /** {@inheritDoc} */
//        @Override public String deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
//            JsonToken startTok = p.getCurrentToken();
//
//            if (startTok.isStructStart()) {
//                StringWriter wrt = new StringWriter(4096);
//
//                JsonGenerator gen = factory.createGenerator(wrt);
//
//                JsonToken tok = startTok, endTok = startTok == START_ARRAY ? END_ARRAY : END_OBJECT;
//
//                int cnt = 1;
//
//                while (cnt > 0) {
//                    writeToken(tok, p, gen);
//
//                    tok = p.nextToken();
//
//                    if (tok == startTok)
//                        cnt++;
//                    else if (tok == endTok)
//                        cnt--;
//                }
//
//                gen.close();
//
//                return wrt.toString();
//            }
//
//            return p.getValueAsString();
//        }
//    }
}
