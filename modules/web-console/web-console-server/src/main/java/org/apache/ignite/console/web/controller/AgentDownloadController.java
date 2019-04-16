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

package org.apache.ignite.console.web.controller;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.ignite.console.dto.Account;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import static org.apache.ignite.console.common.Utils.origin;
import static org.springframework.http.HttpHeaders.CACHE_CONTROL;
import static org.springframework.http.HttpHeaders.CONTENT_DISPOSITION;
import static org.springframework.http.HttpHeaders.EXPIRES;
import static org.springframework.http.HttpHeaders.PRAGMA;

/**
 * Controller for download Web Agent API.
 */
@RestController
public class AgentDownloadController {
    /** Buffer size of 30Mb to handle Web Agent ZIP file manipulations. */
    private static final int BUFFER_SZ = 30 * 1024 * 1024;

    /** */
    @Value("${agent.folder.name}")
    private String agentFolderName;

    /** */
    @Value("${agent.file.name}")
    private String agentFileName;

    /** */
    private final Path pathToAgentZip;

    /**
     * Default constructor.
     */
    public AgentDownloadController() {
        pathToAgentZip = Paths.get(agentFolderName, agentFileName + ".zip");
    }

    /**
     *
     * @param req Request.
     * @param user User.
     * @return Agent ZIP.
     * @throws Exception If failed.
     */
    @GetMapping(path = "/api/v1/downloads/agent")
    private ResponseEntity<Resource> load(HttpServletRequest req, @AuthenticationPrincipal Account user) throws Exception {
        if (!Files.exists(pathToAgentZip))
            throw new FileNotFoundException("Missing agent zip oFn server");

        ZipFile zip = new ZipFile(pathToAgentZip.toFile());

        ByteArrayOutputStream baos = new ByteArrayOutputStream(BUFFER_SZ);

        ZipArchiveOutputStream zos = new ZipArchiveOutputStream(baos);

        // Make a copy of agent ZIP.
        zip.copyRawEntries(zos, rawEntry -> true);

        // Append "default.properties" to agent ZIP.
        zos.putArchiveEntry(new ZipArchiveEntry(agentFileName + "/default.properties"));

        String content = String.join("\n",
            "tokens=" + user.token(),
            "server-uri=" + origin(req),
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

        HttpHeaders headers = new HttpHeaders();
        headers.add(CACHE_CONTROL, "no-cache, no-store, must-revalidate");
        headers.add(PRAGMA, "no-cache");
        headers.add(EXPIRES, "0");
        headers.add(CONTENT_DISPOSITION, "attachment; filename=\"" + agentFileName + ".zip\"");
        headers.setContentLength(data.length);
        headers.setContentType(MediaType.parseMediaType("application/zip"));

        return ResponseEntity.ok()
            .headers(headers)
            .body(new ByteArrayResource(data));
    }
}
