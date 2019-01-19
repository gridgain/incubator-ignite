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

import java.nio.charset.Charset;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import io.vertx.core.AbstractVerticle;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.slf4j.LoggerFactory;

/**
 * Base class for web socket handlers.
 */
abstract class AbstractHandler extends AbstractVerticle {
    /** */
    final IgniteLogger log = new Slf4jLogger(LoggerFactory.getLogger(AbstractHandler.class));

    /** UTF8 charset. */
    private static final Charset UTF8 = Charset.forName("UTF-8");

    /** */
    protected static final String ROUTER_ADDR = "router";

    /** */
    protected static final int FAILED = 500;

//    /** {@inheritDoc} */
//    @SuppressWarnings("unchecked")
//    @Override public final void call(Object... args) {
//        final Ack cb = safeCallback(args);
//
//        args = removeCallback(args);
//
//        try {
//            final Map<String, Object> params;
//
//            if (args == null || args.length == 0)
//                params = Collections.emptyMap();
//            else if (args.length == 1)
//                params = fromJSON(args[0], Map.class);
//            else
//                throw new IllegalArgumentException("Wrong arguments count, must be <= 1: " + Arrays.toString(args));
//
//            if (pool == null)
//                pool = newThreadPool();
//
//            pool.submit(() -> {
//                try {
//                    Object res = execute(params);
//
//                    // TODO IGNITE-6127 Temporary solution until GZip support for socket.io-client-java.
//                    // See: https://github.com/socketio/socket.io-client-java/issues/312
//                    // We can GZip manually for now.
//                    if (res instanceof RestResult) {
//                        RestResult restRes = (RestResult) res;
//
//                        if (restRes.getData() != null) {
//                            ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
//                            Base64OutputStream b64os = new Base64OutputStream(baos, true, 0, null);
//                            GZIPOutputStream gzip = new GZIPOutputStream(b64os);
//
//                            gzip.write(restRes.getData().getBytes(UTF8));
//
//                            gzip.close();
//
//                            restRes.zipData(baos.toString());
//                        }
//                    }
//
//                    cb.call(null, toJSON(res));
//                }
//                catch (Throwable e) {
//                    log.error("Failed to process event in pool", e);
//
//                    cb.call(e, null);
//                }
//            });
//        }
//        catch (Throwable e) {
//            log.error("Failed to process event", e);
//
//            cb.call(e, null);
//        }
//    }

    /**
     * Creates a thread pool that can schedule commands to run after a given delay, or to execute periodically.
     *
     * @return Newly created thread pool.
     */
    protected ExecutorService newThreadPool() {
        return Executors.newSingleThreadExecutor();
    }

//    /**
//     * Execute command with specified arguments.
//     *
//     * @param args Map with method args.
//     */
//    public abstract Object execute(Map<String, Object> args) throws Exception;
}
