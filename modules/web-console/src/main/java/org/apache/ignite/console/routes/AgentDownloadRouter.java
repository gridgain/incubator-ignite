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
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.ignite.Ignite;

import static org.apache.ignite.console.common.Utils.origin;

/**
 * Router to handle REST API to download Web Agent.
 */
public class AgentDownloadRouter extends AbstractRouter {
    /** Buffer size of 30Mb to handle Web Agent ZIP file manipulations. */
    private static final int BUFFER_SZ = 30 * 1024 * 1024;

    /** */
    private final Path pathToAgentZip;

    /** */
    private final String agentFileName;

    /**
     * @param ignite Ignite.
     * @param agentFolderName Folder where agent ZIP stored.
     * @param agentFileName Agent file name.
     */
    public AgentDownloadRouter(Ignite ignite, String agentFolderName, String agentFileName) {
        super(ignite);

        this.agentFileName = agentFileName;

        pathToAgentZip = Paths.get(agentFolderName, agentFileName + ".zip");
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
                if (!Files.exists(pathToAgentZip))
                    throw new FileNotFoundException("Missing agent zip on server");

                ZipFile zip = new ZipFile(pathToAgentZip.toFile());

                ByteArrayOutputStream baos = new ByteArrayOutputStream(BUFFER_SZ);

                ZipArchiveOutputStream zos = new ZipArchiveOutputStream(baos);

                // Make a copy of agent ZIP.
                zip.copyRawEntries(zos, rawEntry -> true);

                // Append "default.properties" to agent ZIP.
                zos.putArchiveEntry(new ZipArchiveEntry(agentFileName + "/default.properties"));

                String content = String.join("\n",
                    "tokens=" + user.principal().getString("token", "MY_TOKEN"), // TODO WC-938 Take token from Account after WC-949 will be merged.
                    "server-uri=" + origin(ctx.request()),
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
                zos.closeArchiveEntry();
                zos.close();

                byte[] data = baos.toByteArray();

                ctx.response()
                    .setChunked(true)
                    .putHeader(HttpHeaders.CONTENT_TYPE, "application/zip")
                    .putHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + agentFileName + ".zip\"")
                    .putHeader(HttpHeaders.TRANSFER_ENCODING, HttpHeaders.CHUNKED)
                    .putHeader(HttpHeaders.CONTENT_LENGTH, Integer.toString(data.length))
                    .write(Buffer.buffer(data))
                    .end();
            }
            catch (Throwable e) {
                sendError(ctx, "Failed to prepare Web Agent archive for download", e);
            }
        }
    }
}
