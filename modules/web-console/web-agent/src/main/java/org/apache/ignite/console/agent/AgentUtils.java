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

package org.apache.ignite.console.agent;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.ProtectionDomain;
import java.util.List;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.log4j.Logger;
import org.eclipse.jetty.util.ssl.SslContextFactory;

/**
 * Utility methods.
 */
public class AgentUtils {
    /** */
    private static final Logger log = Logger.getLogger(AgentUtils.class.getName());

    /** */
    public static final String[] EMPTY = {};

    /**
     * Default constructor.
     */
    private AgentUtils() {
        // No-op.
    }

    /**
     * @param path Path to normalize.
     * @return Normalized file path.
     */
    private static String normalizePath(String path) {
        return path != null ? path.replace('\\', '/') : null;
    }

    /**
     * @return App folder.
     */
    public static File getAgentHome() {
        try {
            ProtectionDomain domain = AgentLauncher.class.getProtectionDomain();

            // Should not happen, but to make sure our code is not broken.
            if (domain == null || domain.getCodeSource() == null || domain.getCodeSource().getLocation() == null) {
                log.warn("Failed to resolve agent jar location!");

                return null;
            }

            // Resolve path to class-file.
            URI classesUri = domain.getCodeSource().getLocation().toURI();

            boolean win = System.getProperty("os.name").toLowerCase().contains("win");

            // Overcome UNC path problem on Windows (http://www.tomergabel.com/JavaMishandlesUNCPathsOnWindows.aspx)
            if (win && classesUri.getAuthority() != null)
                classesUri = new URI(classesUri.toString().replace("file://", "file:/"));

            return new File(classesUri).getParentFile();
        }
        catch (URISyntaxException | SecurityException ignored) {
            log.warn("Failed to resolve agent jar location!");

            return null;
        }
    }

    /**
     * Gets file associated with path.
     * <p>
     * First check if path is relative to agent home.
     * If not, check if path is absolute.
     * If all checks fail, then {@code null} is returned.
     * <p>
     *
     * @param path Path to resolve.
     * @return Resolved path as file, or {@code null} if path cannot be resolved.
     */
    public static File resolvePath(String path) {
        assert path != null;

        File home = getAgentHome();

        if (home != null) {
            File file = new File(home, normalizePath(path));

            if (file.exists())
                return file;
        }

        // 2. Check given path as absolute.
        File file = new File(path);

        if (file.exists())
            return file;

        return null;
    }

    /**
     *
     * @param keyStore Path to key store.
     * @param keyStorePwd Optional key store password.
     * @param trustAll Whether we should trust for self-signed certificate.
     * @param trustStore Path to trust store.
     * @param trustStorePwd Optional trust store passwo5rd.
     * @param ciphers Optional list of enabled cipher suites.
     * @return SSL context factory.
     */
    public static SslContextFactory sslContextFactory(
        String keyStore,
        String keyStorePwd,
        boolean trustAll,
        String trustStore,
        String trustStorePwd,
        List<String> ciphers
    ) {
        SslContextFactory sslCtxFactory = new SslContextFactory();

        if (!F.isEmpty(keyStore)) {
            sslCtxFactory.setKeyStorePath(keyStore);

            if (!F.isEmpty(keyStorePwd))
                sslCtxFactory.setKeyStorePassword(keyStorePwd);
        }

        if (trustAll) {
            sslCtxFactory.setTrustAll(true);
            // Available in Jetty >= 9.4.15.xxxx sslCtxFactory.setHostnameVerifier((hostname, session) -> true);
        }
        else if (!F.isEmpty(trustStore)) {
            sslCtxFactory.setTrustStorePath(trustStore);

            if (!F.isEmpty(trustStorePwd))
                sslCtxFactory.setTrustStorePassword(trustStorePwd);
        }

        if (!F.isEmpty(ciphers))
            sslCtxFactory.setIncludeCipherSuites(ciphers.toArray(EMPTY));

        return  sslCtxFactory;
    }
}
