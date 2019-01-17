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

package org.apache.ignite.web.console.web.server;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.bridge.PermittedOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.sockjs.BridgeEvent;
import io.vertx.ext.web.handler.sockjs.BridgeOptions;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;

import static io.vertx.ext.bridge.BridgeEventType.PUBLISH;
import static io.vertx.ext.bridge.BridgeEventType.REGISTER;
import static io.vertx.ext.bridge.BridgeEventType.SOCKET_CLOSED;

/**
 * Handler
 */
public class WebConsoleHttpServerVerticle extends AbstractVerticle {
    /** */
    private SockJSHandler webSocketsHnd;

    /** {@inheritDoc} */
    @Override public void start() {
        // Create a router object.
        Router router = Router.router(vertx);

        webSocketsHnd = SockJSHandler.create(vertx);

        router.route("/websocket/*").handler(webSocketsHnd);

        router.route("/api/v1/user").handler(this::handleUser);
        router.route("/api/v1/signup").handler(this::handleSignUp);
        router.route("/api/v1/signin").handler(this::handleSignIn);
        router.route("/api/v1/logout").handler(this::handleLogout);
        router.route("/api/v1/password/forgot").handler(this::handlePasswordForgot);
        router.route("/api/v1/password/reset").handler(this::handlePasswordReset);
        router.route("/api/v1/password/validate/token").handler(this::handlePasswordValidateToken);
        router.route("/api/v1/activation/resend").handler(this::handleActivationResend);

        router.route("/api/v1/configuration/clusters").handler(this::handleClusters);

        router.route("/api/v1/activities/page").handler(this::handleActivitiesPage);

        /*
            app.use('/api/v1/admin', _mustAuthenticated, _adminOnly, adminRoute);
            app.use('/api/v1/profile', _mustAuthenticated, profilesRoute);
            app.use('/api/v1/demo', _mustAuthenticated, demoRoute);

            app.all('/api/v1/configuration/*', _mustAuthenticated);

            app.use('/api/v1/configuration/clusters', clustersRoute);
            app.use('/api/v1/configuration/domains', domainsRoute);
            app.use('/api/v1/configuration/caches', cachesRoute);
            app.use('/api/v1/configuration/igfs', igfssRoute);
            app.use('/api/v1/configuration', configurationsRoute);

            app.use('/api/v1/notebooks', _mustAuthenticated, notebooksRoute);
            app.use('/api/v1/downloads', _mustAuthenticated, downloadsRoute);
            app.use('/api/v1/activities', _mustAuthenticated, activitiesRoute);

            '/api/v1/signin'
            '/api/v1/configuration/clusters'

         */

        // Create the HTTP server.
        vertx
            .createHttpServer()
            .requestHandler(router)
            .listen(3000);

        handle();
    }

    /**
     * xxx
     */
    private void handle() {
        BridgeOptions opts = new BridgeOptions()
            .addInboundPermitted(new PermittedOptions().setAddress("to.server"))
            .addOutboundPermitted(new PermittedOptions().setAddress("to.client"));

        webSocketsHnd.bridge(opts, event -> {
            if (event.type() == PUBLISH)
                publishEvent(event);

            if (event.type() == REGISTER)
                registerEvent(event);

            if (event.type() == SOCKET_CLOSED)
                closeEvent(event);

            //обратите внимание, после обработки события
            // должен вызываться говорящий сам за себя метод.
            event.complete(true);
        });
    }

    /**
     *
     * @param evt xxx
     */
    private void registerEvent(BridgeEvent evt) {
        System.out.println("registerEvent");

        JsonObject rMsg = evt.getRawMessage();

        if (rMsg != null && "web-agent".equals(rMsg.getString("address"))) {
            System.out.println(rMsg);
            // new Thread(() -> vertx.eventBus().publish("to.client", "{\"id\": 1}")).start();
        }
    }

    /**
     * @param evt xxx
     */
    private void closeEvent(BridgeEvent evt) {
        System.out.println("closeEvent: " + evt);

        new Thread(() -> vertx.eventBus().publish("to.client", "{\"id\": 3}")).start();
    }

    /**
     *
     * @param evt xxx
     * @return {@code true}
     */
    private boolean publishEvent(BridgeEvent evt) {
        System.out.println("publishEvent: " + evt);

        JsonObject rMsg = evt.getRawMessage();

        if (rMsg != null && "to.server".equals(rMsg.getString("address"))) {
            String msg = rMsg.getString("body");

            System.out.println(msg);

            vertx.eventBus().publish("to.client", "{\"id\": 2}");

            return true;
        }
        else
            return false;
    }

    /**
     * @param ctx Context
     */
    private void handleUser(RoutingContext ctx) {
        System.out.println(ctx.request().uri());

        HttpServerResponse res = ctx.response();

        res
            .putHeader("content-type", "application/json")
            .end("{\"_id\":\"5683a8e9824d152c044e6281\",\"email\":\"kuaw26@mail.ru\",\"firstName\":\"Alexey\",\"lastName\":\"Kuznetsov\",\"company\":\"GridGain\",\"country\":\"Russia\",\"industry\":\"Other\",\"admin\":true,\"token\":\"NEHYtRKsPHhXT5rrIOJ4\",\"registered\":\"2017-12-21T16:14:37.369Z\",\"lastLogin\":\"2019-01-16T03:51:05.479Z\",\"lastActivity\":\"2019-01-16T03:51:06.084Z\",\"lastEvent\":\"2018-05-23T12:26:29.570Z\",\"demoCreated\":true}");
    }

    /**
     * @param ctx Context
     */
    private void handleSignUp(RoutingContext ctx) {
        System.out.println(ctx.request().uri());

        HttpServerResponse res = ctx.response();

        res.
            setStatusCode(200);
    }

    /**
     * @param ctx Context
     */
    private void handleSignIn(RoutingContext ctx) {
        System.out.println(ctx.request().uri());

        HttpServerResponse res = ctx.response();

        res
            .putHeader("content-type", "application/json")
            .end("{\"_id\":\"5683a8e9824d152c044e6281\"}");
    }

    /**
     * @param ctx Context
     */
    private void handleLogout(RoutingContext ctx) {
        System.out.println(ctx.request().uri());

        HttpServerResponse res = ctx.response();

        res
            .putHeader("content-type", "application/json")
            .end("{\"_id\":\"5683a8e9824d152c044e6281\"}");
    }

    /**
     * @param ctx Context
     */
    private void handlePasswordForgot(RoutingContext ctx) {
        System.out.println(ctx.request().uri());

        HttpServerResponse res = ctx.response();

        res
            .putHeader("content-type", "application/json")
            .end("{\"_id\":\"5683a8e9824d152c044e6281\"}");
    }

    /**
     * @param ctx Context
     */
    private void handlePasswordReset(RoutingContext ctx) {
        System.out.println(ctx.request().uri());

        HttpServerResponse res = ctx.response();

        res
            .putHeader("content-type", "application/json")
            .end("{\"_id\":\"5683a8e9824d152c044e6281\"}");
    }

    /**
     * @param ctx Context
     */
    private void handlePasswordValidateToken(RoutingContext ctx) {
        System.out.println(ctx.request().uri());

        HttpServerResponse res = ctx.response();

        res
            .putHeader("content-type", "application/json")
            .end("{\"_id\":\"5683a8e9824d152c044e6281\"}");
    }

    /**
     * @param ctx Context
     */
    private void handleActivationResend(RoutingContext ctx) {
        System.out.println(ctx.request().uri());

        HttpServerResponse res = ctx.response();

        res
            .putHeader("content-type", "application/json")
            .end("{\"_id\":\"5683a8e9824d152c044e6281\"}");
    }

    /**
     * @param ctx Context
     */
    private void handleClusters(RoutingContext ctx) {
        System.out.println(ctx.request().uri());

        HttpServerResponse res = ctx.response();

        res
            .putHeader("content-type", "application/json")
            .end("[{\"_id\":\"5b9b5ad477670d001936692a\",\"name\":\"LOST_AND_FOUND\",\"discovery\":\"Multicast\",\"cachesCount\":0,\"modelsCount\":0,\"igfsCount\":0,\"gridgainInfo\":{\"gridgainEnabled\":true,\"rollingUpdatesEnabled\":true,\"securityEnabled\":true,\"dataReplicationReceiverEnabled\":false,\"dataReplicationSenderEnabled\":false,\"snapshotsEnabled\":false}},{\"_id\":\"5c340a488438de7b2eb2a4ba\",\"name\":\"Cluster1\",\"discovery\":\"Multicast\",\"cachesCount\":0,\"modelsCount\":0,\"igfsCount\":0,\"gridgainInfo\":{\"gridgainEnabled\":false,\"rollingUpdatesEnabled\":false,\"securityEnabled\":false,\"dataReplicationReceiverEnabled\":false,\"dataReplicationSenderEnabled\":false,\"snapshotsEnabled\":false}}]");
    }

    /**
     * @param ctx Context
     */
    private void handleActivitiesPage(RoutingContext ctx) {
        System.out.println(ctx.request().uri());

        HttpServerResponse res = ctx.response();

        res
            .putHeader("content-type", "application/json")
            .end("{\"_id\":\"5c340a44b46d730801ad839f\",\"action\":\"base.configuration.overview\",\"date\":\"2019-01-01T00:00:00.000Z\",\"owner\":\"5683a8e9824d152c044e6281\",\"__v\":0,\"group\":\"configuration\",\"amount\":4}");
    }
}
