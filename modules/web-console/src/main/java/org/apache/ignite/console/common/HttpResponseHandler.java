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

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.ext.web.RoutingContext;
import org.apache.ignite.Ignite;

import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static org.apache.ignite.console.common.Utils.sendError;
import static org.apache.ignite.console.common.Utils.sendResult;

/**
 * Reply handler that send HTTP response.
 */
public class HttpResponseHandler<T> implements Handler<AsyncResult<Message<T>>> {
    /** */
    private final Ignite ignite;

    /** */
    private final RoutingContext ctx;

    /** */
    private final String errMsg;

    /**
     * @param ignite Ignite.
     * @param ctx Context.
     * @param errMsg Error message.
     */
    public HttpResponseHandler(Ignite ignite, RoutingContext ctx, String errMsg) {
        this.ignite = ignite;
        this.ctx = ctx;
        this.errMsg = errMsg;
    }

    /** {@inheritDoc} */
    @Override public void handle(AsyncResult<Message<T>> asyncRes) {
        if (asyncRes.succeeded())
            sendResult(ctx, asyncRes.result().body());
        else {
            ignite.log().error(errMsg, asyncRes.cause());

            sendError(ctx, HTTP_INTERNAL_ERROR, errMsg, asyncRes.cause());
        }
    }
}
