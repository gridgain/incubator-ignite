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
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.regex.Pattern;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.internal.util.typedef.F;
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
import static org.apache.ignite.internal.util.io.GridFilenameUtils.removeExtension;
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
    private static final Comparator<File> LATEST_FILE = new Comparator<File>() {
        /** {@inheritDoc} */
        @Override public int compare(File f1, File f2) {
            return Long.compare(f1.lastModified(), f2.lastModified());
        }
    };

    /** */
    @Value("${agent.folder.name}")
    private String agentFolderName;

    /** */
    @Value("${agent.file.mask}")
    private String agentFileMask;

    /**
     * @param req Request.
     * @param user User.
     * @return Agent ZIP.
     * @throws Exception If failed.
     */
    @GetMapping(path = "/api/v1/downloads/agent")
    private ResponseEntity<Resource> load(HttpServletRequest req, @AuthenticationPrincipal Account user) throws Exception {
        File agentFolder = new File(agentFolderName);

        Pattern ptrn = Pattern.compile(agentFileMask);

        File[] files = agentFolder.listFiles(new FileFilter() {
            @Override public boolean accept(File file) {
                return !file.isDirectory() && ptrn.matcher(file.getName()).matches();
            }
        });

        if (F.isEmpty(files))
            throw new FileNotFoundException("Web Console Agent distributive not found on server");

        Arrays.sort(files, LATEST_FILE.reversed());

        File latestAgentFile = files[0];

        ZipFile zip = new ZipFile(latestAgentFile);

        ByteArrayOutputStream baos = new ByteArrayOutputStream(BUFFER_SZ);

        ZipArchiveOutputStream zos = new ZipArchiveOutputStream(baos);

        // Make a copy of agent ZIP.
        zip.copyRawEntries(zos, rawEntry -> true);

        // Append "default.properties" to agent ZIP.
        zos.putArchiveEntry(new ZipArchiveEntry(removeExtension(latestAgentFile.getName()) + "/default.properties"));

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
        headers.add(CONTENT_DISPOSITION, "attachment; filename=\"" + latestAgentFile.getName());
        headers.setContentLength(data.length);
        headers.setContentType(MediaType.parseMediaType("application/zip"));

        return ResponseEntity.ok()
            .headers(headers)
            .body(new ByteArrayResource(data));
    }
}
