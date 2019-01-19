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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.console.agent.AgentConfiguration;
import org.apache.ignite.console.agent.db.DbDriver;
import org.apache.ignite.console.agent.db.DbMetadataReader;
import org.apache.ignite.console.agent.db.DbSchema;
import org.apache.ignite.console.agent.db.DbTable;
import org.apache.ignite.console.demo.AgentMetadataDemo;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.console.agent.AgentUtils.GENERAL_ERR_CODE;
import static org.apache.ignite.console.agent.AgentUtils.resolvePath;
import static org.apache.ignite.console.agent.AgentUtils.toJSON;

/**
 * API to extract database metadata.
 */
public class DatabaseListener extends AbstractVerticle {
    /** */
    private static final IgniteLogger log = new Slf4jLogger(LoggerFactory.getLogger(DatabaseListener.class));

    /** */
    private static final String EVENT_SCHEMA_IMPORT_DRIVERS = "schemaImport:drivers";

    /** */
    private static final String EVENT_SCHEMA_IMPORT_SCHEMAS = "schemaImport:schemas";

    /** */
    private static final String EVENT_SCHEMA_IMPORT_METADATA = "schemaImport:metadata";

    /** */
    private final File driversFolder;

    /** */
    private final DbMetadataReader dbMetaReader;

    /**
     * @param cfg Config.
     */
    public DatabaseListener(AgentConfiguration cfg) {
        driversFolder = resolvePath(F.isEmpty(cfg.driversFolder()) ? "jdbc-drivers" : cfg.driversFolder());

        dbMetaReader = new DbMetadataReader();
    }

    /** {@inheritDoc} */
    @Override public void start() {
        vertx.eventBus().consumer(EVENT_SCHEMA_IMPORT_DRIVERS, this::driversHandler);
        vertx.eventBus().consumer(EVENT_SCHEMA_IMPORT_SCHEMAS, this::schemasHandler);
        vertx.eventBus().consumer(EVENT_SCHEMA_IMPORT_METADATA, this::metadataHandler);
    }

    /**
     * @param msg Message.
     */
    private void driversHandler(Message<Void> msg) {
        try {
            List<DbDriver> drivers = new ArrayList<>();

            if (driversFolder != null) {
                log.info("Collecting JDBC drivers in folder: " + driversFolder.getPath());

                File[] list = driversFolder.listFiles((dir, name) -> name.endsWith(".jar"));

                if (list != null) {
                    for (File file : list) {
                        try {
                            boolean win = System.getProperty("os.name").contains("win");

                            URL url = new URL("jar", null,
                                "file:" + (win ? "/" : "") + file.getPath() + "!/META-INF/services/java.sql.Driver");

                            try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream(), UTF_8))) {
                                String jdbcDriverCls = reader.readLine();

                                drivers.add(new DbDriver(file.getName(), jdbcDriverCls));

                                log.info("Found: [driver=" + file + ", class=" + jdbcDriverCls + "]");
                            }
                        }
                        catch (IOException e) {
                            drivers.add(new DbDriver(file.getName(), null));

                            log.info("Found: [driver=" + file + "]");
                            log.error("Failed to detect driver class: " + e.getMessage());
                        }
                    }
                }
                else
                    log.info("JDBC drivers folder has no files, returning empty list");
            }
            else
                log.info("JDBC drivers folder not specified, returning empty list");

            msg.reply(toJSON(EVENT_SCHEMA_IMPORT_DRIVERS, drivers));
        }
        catch (Throwable e) {
            msg.fail(GENERAL_ERR_CODE, e.getMessage());
        }
    }

    /**
     * @param msg Message.
     */
    private void schemasHandler(Message<Map<String, Object>> msg) {
        try {
            String jdbcDriverJarPath = null;

            Map<String, Object> args = msg.body();

            if (args.containsKey("jdbcDriverJar"))
                jdbcDriverJarPath = args.get("jdbcDriverJar").toString();

            if (!args.containsKey("jdbcDriverClass"))
                throw new IllegalArgumentException("Missing driverClass in arguments: " + args);

            String jdbcDriverCls = args.get("jdbcDriverClass").toString();

            if (!args.containsKey("jdbcUrl"))
                throw new IllegalArgumentException("Missing url in arguments: " + args);

            String jdbcUrl = args.get("jdbcUrl").toString();

            if (!args.containsKey("info"))
                throw new IllegalArgumentException("Missing info in arguments: " + args);

            Properties jdbcInfo = new Properties();

            jdbcInfo.putAll((Map)args.get("info"));

            log.info("Start collecting database schemas [drvJar=" + jdbcDriverJarPath +
                ", drvCls=" + jdbcDriverCls + ", jdbcUrl=" + jdbcUrl + "]");

            try (Connection conn = connect(jdbcDriverJarPath, jdbcDriverCls, jdbcUrl, jdbcInfo)) {
                String catalog = conn.getCatalog();

                if (catalog == null) {
                    String[] parts = jdbcUrl.split("[/:=]");

                    catalog = parts.length > 0 ? parts[parts.length - 1] : "NONE";
                }

                Collection<String> schemas = dbMetaReader.schemas(conn);

                log.info("Collected database schemas [jdbcUrl=" + jdbcUrl + ", catalog=" + catalog +
                    ", count=" + schemas.size() + "]");

                msg.reply(toJSON(EVENT_SCHEMA_IMPORT_SCHEMAS, new DbSchema(catalog, schemas)));
            }
        }
        catch (Throwable e) {
            msg.fail(GENERAL_ERR_CODE, "Failed to collect DB schemas");
        }
    }

    /**
     * @param msg Message.
     */
    private void metadataHandler(Message<Map<String, Object>> msg) {
        try {
            String jdbcDriverJarPath = null;

            Map<String, Object> args = msg.body();

            if (args.containsKey("jdbcDriverJar"))
                jdbcDriverJarPath = args.get("jdbcDriverJar").toString();

            if (!args.containsKey("jdbcDriverClass"))
                throw new IllegalArgumentException("Missing driverClass in arguments: " + args);

            String jdbcDriverCls = args.get("jdbcDriverClass").toString();

            if (!args.containsKey("jdbcUrl"))
                throw new IllegalArgumentException("Missing url in arguments: " + args);

            String jdbcUrl = args.get("jdbcUrl").toString();

            if (!args.containsKey("info"))
                throw new IllegalArgumentException("Missing info in arguments: " + args);

            Properties jdbcInfo = new Properties();

            jdbcInfo.putAll((Map)args.get("info"));

            if (!args.containsKey("schemas"))
                throw new IllegalArgumentException("Missing schemas in arguments: " + args);

            List<String> schemas = (List<String>)args.get("schemas");

            if (!args.containsKey("tablesOnly"))
                throw new IllegalArgumentException("Missing tablesOnly in arguments: " + args);

            boolean tblsOnly = (boolean)args.get("tablesOnly");

            log.info("Start collecting database metadata [drvJar=" + jdbcDriverJarPath +
                ", drvCls=" + jdbcDriverCls + ", jdbcUrl=" + jdbcUrl + "]");

            try (Connection conn = connect(jdbcDriverJarPath, jdbcDriverCls, jdbcUrl, jdbcInfo)) {
                Collection<DbTable> metadata = dbMetaReader.metadata(conn, schemas, tblsOnly);

                log.info("Collected database metadata [jdbcUrl=" + jdbcUrl + ", count=" + metadata.size() + "]");

                msg.reply(toJSON(EVENT_SCHEMA_IMPORT_METADATA, metadata));
            }
        }
        catch (Throwable e) {
            msg.fail(GENERAL_ERR_CODE, "Failed to collect DB metadata");
        }
    }

    /**
     * @param jdbcDriverJarPath JDBC driver JAR path.
     * @param jdbcDriverCls JDBC driver class.
     * @param jdbcUrl JDBC URL.
     * @param jdbcInfo Properties to connect to database.
     * @return Connection to database.
     * @throws SQLException If failed to connect.
     */
    private Connection connect(String jdbcDriverJarPath, String jdbcDriverCls, String jdbcUrl,
        Properties jdbcInfo) throws SQLException {
        if (AgentMetadataDemo.isTestDriveUrl(jdbcUrl))
            return AgentMetadataDemo.testDrive();

        if (!new File(jdbcDriverJarPath).isAbsolute() && driversFolder != null)
            jdbcDriverJarPath = new File(driversFolder, jdbcDriverJarPath).getPath();

        return dbMetaReader.connect(jdbcDriverJarPath, jdbcDriverCls, jdbcUrl, jdbcInfo);
    }
}
