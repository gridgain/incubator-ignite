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

package org.apache.ignite.console.routes;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.apache.ignite.Ignite;

/**
 * Router to handle REST API to download Web Agent.
 */
public class AgentDownloadRouter extends AbstractRouter {
    /** Buffer size of 30Mb to handle Web Agent ZIP file manipulations. */
    private static final int BUFFER_SZ = 30 * 1024 * 1024;

    /** */
    private File pathToWebAgentZip;

    /** */
    private final String file;

    /**
     * @param ignite Ignite.
     */
    public AgentDownloadRouter(Ignite ignite, String folder, String file) {
        super(ignite);

        this.file = file;
        this.pathToWebAgentZip = Paths.get(folder, file + ".zip").toFile();
    }

    /** {@inheritDoc} */
    @Override protected void initializeCaches() {
        // No-op so far.
    }

    /** {@inheritDoc} */
    @Override public void install(Router router) {
        router.get("/api/v1/downloads/agent").handler(this::load);
    }

    /**
     * @param ctx Context.
     */
    private void load(RoutingContext ctx) {
        User user = checkUser(ctx);

        if (user != null) {
            try {
                ZipFile zip = new ZipFile(pathToWebAgentZip);

                ByteArrayOutputStream baos = new ByteArrayOutputStream(BUFFER_SZ);

                ZipOutputStream zos = new ZipOutputStream(baos);

                Enumeration<? extends ZipEntry> entries = zip.entries();

                byte[] buf = new byte[BUFFER_SZ];

                while (entries.hasMoreElements()) {
                    ZipEntry e = entries.nextElement();

                    zos.putNextEntry(e);

                    if (!e.isDirectory()) {
                        int bytesRead;

                        InputStream is = zip.getInputStream(e);

                        while ((bytesRead = is.read(buf)) != -1)
                            zos.write(buf, 0, bytesRead);
                    }

                    zos.closeEntry();
                }

                // Append default.properties to ZIP.
                ZipEntry e = new ZipEntry(file + "/default.properties");
                zos.putNextEntry(e);

                String content = String.join("\n",
                    "tokens=" + user.principal().getString("token", "MY_TOKEN"),
                    "server-uri=" + ctx.request().host(),
                    "#Uncomment following options if needed:",
                    "#node-uri=http://localhost:8080",
                    "#node-login=ignite",
                    "#node-password=ignite",
                    "#driver-folder=./jdbc-drivers",
                    "#Uncomment and configure following SSL options if needed:",
                    "#node-key-store=client.jks",
                    "#node-key-store-password=MY_PASSWORD",
                    "#node-trust-store=ca.jks",
                    "#node-trust-store-password=MY_PASSWORD",
                    "#server-key-store=client.jks",
                    "#server-key-store-password=MY_PASSWORD",
                    "#server-trust-store=ca.jks",
                    "#server-trust-store-password=MY_PASSWORD",
                    "#cipher-suites=CIPHER1,CIPHER2,CIPHER3"
                );

                zos.write(content.getBytes());
                zos.closeEntry();
                zos.close();

                byte[] data = baos.toByteArray();

                HttpServerResponse res = ctx.response()
                    .setChunked(true)
                    .putHeader(HttpHeaders.CONTENT_TYPE, "application/zip")
                    .putHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + file + ".zip\"")
                    .putHeader(HttpHeaders.TRANSFER_ENCODING, HttpHeaders.CHUNKED)
                    .putHeader(HttpHeaders.CONTENT_LENGTH, Integer.toString(data.length));

                res.write(Buffer.buffer(data));
                res.end();
            }
            catch (Throwable e) {
                sendError(ctx, "Failed to prepare Web Agent archive", e);
            }
        }
    }
}
