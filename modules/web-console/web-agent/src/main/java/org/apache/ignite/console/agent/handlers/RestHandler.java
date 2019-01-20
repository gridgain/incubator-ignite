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

package org.apache.ignite.console.agent.handlers;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.net.JksOptions;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.console.agent.AgentConfiguration;
import org.apache.ignite.internal.processors.rest.protocols.http.jetty.GridJettyObjectMapper;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.slf4j.LoggerFactory;

import static com.fasterxml.jackson.core.JsonToken.END_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.END_OBJECT;
import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static org.apache.ignite.console.agent.handlers.Addresses.EVENT_NODE_REST;
import static org.apache.ignite.console.agent.handlers.Addresses.EVENT_NODE_VISOR_TASK;
import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_AUTH_FAILED;
import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_FAILED;
import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_SUCCESS;

/**
 * API to translate REST requests to Ignite cluster.
 */
public class RestHandler extends AbstractVerticle {
    /** */
    private static final IgniteLogger log = new Slf4jLogger(LoggerFactory.getLogger(RestHandler.class));

    /** */
    private final AgentConfiguration cfg;

    /** JSON object mapper. */
    private static final ObjectMapper MAPPER = new GridJettyObjectMapper();

    /** */
    private WebClient webClient;

    /** Index of alive node URI. */
    private final Map<List<String>, Integer> startIdxs = U.newHashMap(2);


    /**
     * @param cfg Config.
     */
    public RestHandler(AgentConfiguration cfg) {
        this.cfg = cfg;
    }

    /** {@inheritDoc} */
    @Override public void start() {
        boolean nodeTrustAll = Boolean.getBoolean("trust.all");
        boolean hasNodeTrustStore = cfg.nodeTrustStore() != null;

        if (nodeTrustAll && hasNodeTrustStore) {
            log.warning("Options contains both '--node-trust-store' and '-Dtrust.all=true'. " +
                "Option '-Dtrust.all=true' will be ignored on connect to cluster.");

            nodeTrustAll = false;
        }

        WebClientOptions httpOptions = new WebClientOptions();

        boolean ssl = nodeTrustAll || hasNodeTrustStore || cfg.nodeKeyStore() != null;

        if (ssl) {
            httpOptions
                .setSsl(true)
                .setTrustAll(nodeTrustAll)
                .setKeyStoreOptions(new JksOptions()
                    .setPath(cfg.nodeKeyStore())
                    .setPassword(cfg.nodeKeyStorePassword()))
                .setTrustStoreOptions(new JksOptions()
                    .setPath(cfg.nodeTrustStore())
                    .setPassword(cfg.nodeTrustStorePassword()));

            cfg.cipherSuites().forEach(httpOptions::addEnabledCipherSuite);
        }

        webClient = WebClient.create(vertx, httpOptions);

        EventBus eventBus = vertx.eventBus();

        eventBus.consumer(EVENT_NODE_REST, this::handleNodeRest);
        eventBus.consumer(EVENT_NODE_VISOR_TASK, this::handleNodeVisor);
    }

    /**
     * @param msg Message.
     */
    private void handleNodeRest(Message<Void> msg) {

    }

    /**
     * @param msg Message.
     */
    private void handleNodeVisor(Message<Void> msg) {

    }

//    /** {@inheritDoc} */
//    public Object execute(Map<String, Object> args) {
//        if (log.isDebugEnabled())
//            log.debug("Start parse REST command args: " + args);
//
//        Map<String, Object> params = null;
//
//        if (args.containsKey("params"))
//            params = (Map<String, Object>)args.get("params");
//
//        if (!args.containsKey("demo"))
//            throw new IllegalArgumentException("Missing demo flag in arguments: " + args);
//
//        boolean demo = (boolean)args.get("demo");
//
//        if (F.isEmpty((String)args.get("token")))
//            return RestResult.fail(401, "Request does not contain user token.");
//
//        Map<String, Object> headers = null;
//
//        if (args.containsKey("headers"))
//            headers = (Map<String, Object>)args.get("headers");
//
//        try {
//            if (demo) {
//                if (AgentClusterDemo.getDemoUrl() == null) {
//                    AgentClusterDemo.tryStart().await();
//
//                    if (AgentClusterDemo.getDemoUrl() == null)
//                        return RestResult.fail(404, "Failed to send request because of embedded node for demo mode is not started yet.");
//                }
//
//                return null; // restExecutor.sendRequest(AgentClusterDemo.getDemoUrl(), params, headers);
//            }
//
//            return null; // restExecutor.sendRequest(this.cfg.nodeURIs(), params, headers);
//        }
//        catch (Throwable e) {
//            U.error(log, "Failed to execute REST command with parameters: " + params, e);
//
//            return RestResult.fail(404, e.getMessage());
//        }
//    }


    /** */
    private RestResult parseResponse(AsyncResult<HttpResponse<Buffer>> asyncRes) {
    }

    /** */
    private Future<RestResult> sendRequest(String url, Map<String, Object> params) throws Exception {
        Future<RestResult> fut = Future.future();

        URI uri = new URI(url);

        HttpRequest<Buffer> req = webClient.post(uri.getPort(), uri.getHost(), "/ignite");

        params.forEach((key, value) -> req.addQueryParam(key, value.toString()));

        req.send(asyncRes -> {
            HttpResponse<Buffer> response = asyncRes.result();

            RestResult res;

            if (asyncRes.succeeded()) {
                try {
                    RestResponseHolder holder = MAPPER.readValue(response.body().getBytes(), RestResponseHolder.class);

                    int status = holder.getSuccessStatus();

                    res = status == STATUS_SUCCESS
                        ? RestResult.success(holder.getResponse(), holder.getSessionToken())
                        : RestResult.fail(status, holder.getError());
                }
                catch (Throwable e) {
                    res = RestResult.fail(STATUS_FAILED, e.getMessage());
                }

                if (response.statusCode() == 401)
                    return RestResult.fail(STATUS_AUTH_FAILED, "Failed to authenticate in cluster. " +
                        "Please check agent\'s login and password or node port.");

                if (response.statusCode() == 404)
                    return RestResult.fail(STATUS_FAILED, "Failed connect to cluster.");

                return RestResult.fail(STATUS_FAILED, "Failed to execute REST command: " + response);
            }
            else
                fut.fail(asyncRes.cause());
        });

        return fut;
    }

//    /**
//     * Send request to cluster.
//     *
//     * @param nodeURIs List of cluster nodes URIs.
//     * @param params Map with request params.
//     * @return Response from cluster.
//     * @throws IOException If failed to send request to cluster.
//     */
//    public RestResult sendRequest(List<String> nodeURIs, Map<String, Object> params) throws IOException {
//        Integer startIdx = startIdxs.getOrDefault(nodeURIs, 0);
//
//        int urlsCnt = nodeURIs.size();
//
//        for (int i = 0;  i < urlsCnt; i++) {
//            Integer currIdx = (startIdx + i) % urlsCnt;
//
//            String nodeUrl = nodeURIs.get(currIdx);
//
//            try {
//                RestResult res = sendRequest(nodeUrl, params);
//
//                // If first attempt failed then throttling should be cleared.
//                if (i > 0)
//                    LT.clear();
//
//                LT.info(log, "Connected to cluster [url=" + nodeUrl + "]");
//
//                startIdxs.put(nodeURIs, currIdx);
//
//                return res;
//            }
//            catch (ConnectException ignored) {
//                LT.warn(log, "Failed connect to cluster [url=" + nodeUrl + "]");
//            }
//        }
//
//        LT.warn(log, "Failed connect to cluster. " +
//            "Please ensure that nodes have [ignite-rest-http] module in classpath " +
//            "(was copied from libs/optional to libs folder).");
//
//        throw new ConnectException("Failed connect to cluster [urls=" + nodeURIs + ", parameters=" + params + "]");
//    }

    /**
     * REST response holder Java bean.
     */
    private static class RestResponseHolder {
        /** Success flag */
        private int successStatus;

        /** Error. */
        private String err;

        /** Response. */
        private String res;

        /** Session token string representation. */
        private String sesTok;

        /**
         * @return {@code True} if this request was successful.
         */
        public int getSuccessStatus() {
            return successStatus;
        }

        /**
         * @param successStatus Whether request was successful.
         */
        public void setSuccessStatus(int successStatus) {
            this.successStatus = successStatus;
        }

        /**
         * @return Error.
         */
        public String getError() {
            return err;
        }

        /**
         * @param err Error.
         */
        public void setError(String err) {
            this.err = err;
        }

        /**
         * @return Response object.
         */
        public String getResponse() {
            return res;
        }

        /**
         * @param res Response object.
         */
        @JsonDeserialize(using = RawContentDeserializer.class)
        public void setResponse(String res) {
            this.res = res;
        }

        /**
         * @return String representation of session token.
         */
        public String getSessionToken() {
            return sesTok;
        }

        /**
         * @param sesTok String representation of session token.
         */
        public void setSessionToken(String sesTok) {
            this.sesTok = sesTok;
        }
    }

    /**
     * Raw content deserializer that will deserialize any data as string.
     */
    private static class RawContentDeserializer extends JsonDeserializer<String> {
        /** */
        private final JsonFactory factory = new JsonFactory();

        /**
         * @param tok Token to process.
         * @param p Parser.
         * @param gen Generator.
         */
        private void writeToken(JsonToken tok, JsonParser p, JsonGenerator gen) throws IOException {
            switch (tok) {
                case FIELD_NAME:
                    gen.writeFieldName(p.getText());
                    break;

                case START_ARRAY:
                    gen.writeStartArray();
                    break;

                case END_ARRAY:
                    gen.writeEndArray();
                    break;

                case START_OBJECT:
                    gen.writeStartObject();
                    break;

                case END_OBJECT:
                    gen.writeEndObject();
                    break;

                case VALUE_NUMBER_INT:
                    gen.writeNumber(p.getBigIntegerValue());
                    break;

                case VALUE_NUMBER_FLOAT:
                    gen.writeNumber(p.getDecimalValue());
                    break;

                case VALUE_TRUE:
                    gen.writeBoolean(true);
                    break;

                case VALUE_FALSE:
                    gen.writeBoolean(false);
                    break;

                case VALUE_NULL:
                    gen.writeNull();
                    break;

                default:
                    gen.writeString(p.getText());
            }
        }

        /** {@inheritDoc} */
        @Override public String deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            JsonToken startTok = p.getCurrentToken();

            if (startTok.isStructStart()) {
                StringWriter wrt = new StringWriter(4096);

                JsonGenerator gen = factory.createGenerator(wrt);

                JsonToken tok = startTok, endTok = startTok == START_ARRAY ? END_ARRAY : END_OBJECT;

                int cnt = 1;

                while (cnt > 0) {
                    writeToken(tok, p, gen);

                    tok = p.nextToken();

                    if (tok == startTok)
                        cnt++;
                    else if (tok == endTok)
                        cnt--;
                }

                gen.close();

                return wrt.toString();
            }

            return p.getValueAsString();
        }
    }

}
