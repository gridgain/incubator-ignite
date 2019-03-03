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

import java.util.function.Function;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

/**
 * Blocking code handler boilerplate.
 */
public class BlockingHandler<T> implements Handler<Future<T>> {
    /** */
    private final Message<JsonObject> msg;

    /** */
    private final Function<Message<JsonObject>, T> supplier;

    /**
     * @param msg Message to process.
     * @param supplier Data supplier.
     */
    public BlockingHandler(Message<JsonObject> msg, Function<Message<JsonObject>, T> supplier) {
        this.msg = msg;
        this.supplier = supplier;
    }

    /** {@inheritDoc} */
    @Override public void handle(Future<T> fut) {
        try {
            fut.complete(supplier.apply(msg));
        }
        catch (Throwable e) {
            fut.fail(e);
        }
    }
}
