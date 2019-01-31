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

package org.apache.ignite.console.verticles;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.ext.auth.PRNG;

/**
 *
 */
public class CryptoVerticle extends AbstractVerticle {
    /** */
    private PRNG rnd;

    /** {@inheritDoc} */
    @Override public void start() throws Exception {
        rnd = new PRNG(vertx);

        EventBus bus = vertx.eventBus();

        bus.consumer("crypto:salt", this::handleSalt);
        bus.consumer("crypto:hash", this::handleHash);
    }

    /**
     *
     * @param msg
     */
    private void handleHash(Message<String> msg) {
        msg.reply("Hash!");
    }

    /**
     * @param msg Message.
     */
    private void handleSalt(Message<String> msg) {
        msg.reply("Salt!");
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        rnd.close();
    }
}
