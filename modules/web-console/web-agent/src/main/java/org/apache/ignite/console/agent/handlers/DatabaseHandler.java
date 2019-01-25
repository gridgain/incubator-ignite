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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.console.agent.AgentConfiguration;
import org.apache.ignite.console.agent.db.DbMetadataReader;
import org.apache.ignite.console.agent.db.DbSchema;
import org.apache.ignite.console.agent.db.DbTable;
import org.apache.ignite.console.demo.AgentMetadataDemo;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.slf4j.LoggerFactory;

import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.console.agent.AgentUtils.resolvePath;

/**
 * Handler extract database metadata for "Metadata import" dialog on Web Console.
 */
public class DatabaseHandler extends AbstractVerticle {
    /** */
    private static final IgniteLogger log = new Slf4jLogger(LoggerFactory.getLogger(DatabaseHandler.class));

    /** */
    private final File driversFolder;

    /** */
    private final DbMetadataReader dbMetaReader;

    /**
     * @param cfg Config.
     */
    public DatabaseHandler(AgentConfiguration cfg) {
        driversFolder = resolvePath(F.isEmpty(cfg.driversFolder()) ? "jdbc-drivers" : cfg.driversFolder());

        dbMetaReader = new DbMetadataReader();
    }

    /** {@inheritDoc} */
    @Override public void start() {
        EventBus eventBus = vertx.eventBus();

        eventBus.consumer(Addresses.SCHEMA_IMPORT_DRIVERS, this::driversHandler);
        eventBus.consumer(Addresses.SCHEMA_IMPORT_SCHEMAS, this::schemasHandler);
        eventBus.consumer(Addresses.SCHEMA_IMPORT_METADATA, this::metadataHandler);
    }

    /**
     * @param jdbcDriverJar File name of driver jar file.
     * @param jdbcDriverCls Optional JDBC driver class name.
     * @return JSON for driver info.
     */
    private JsonObject driver(String jdbcDriverJar, String jdbcDriverCls) {
        return new JsonObject()
            .put("jdbcDriverJar", jdbcDriverJar)
            .put("jdbcDriverClass", jdbcDriverCls);
    }

    /**
     * @param msg Message.
     */
    private void driversHandler(Message<Void> msg) {
        try {
            JsonArray drivers = new JsonArray();

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

                                drivers.add(driver(file.getName(), jdbcDriverCls));

                                log.info("Found: [driver=" + file + ", class=" + jdbcDriverCls + "]");
                            }
                        }
                        catch (IOException e) {
                            drivers.add(driver(file.getName(), null));

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

            msg.reply(drivers);
        }
        catch (Throwable e) {
            msg.fail(HTTP_INTERNAL_ERROR, e.getMessage());
        }
    }

    /**
     * @param msg Message.
     */
    private void schemasHandler(Message<JsonObject> msg) {
        try {
            String jdbcDriverJarPath = null;

            JsonObject args = msg.body();

            if (args.containsKey("jdbcDriverJar"))
                jdbcDriverJarPath = args.getString("jdbcDriverJar");

            if (!args.containsKey("jdbcDriverClass"))
                throw new IllegalArgumentException("Missing driverClass in arguments: " + args);

            String jdbcDriverCls = args.getString("jdbcDriverClass");

            if (!args.containsKey("jdbcUrl"))
                throw new IllegalArgumentException("Missing url in arguments: " + args);

            String jdbcUrl = args.getString("jdbcUrl");

            if (!args.containsKey("info"))
                throw new IllegalArgumentException("Missing info in arguments: " + args);

            Properties jdbcInfo = new Properties();

            jdbcInfo.putAll(args.getJsonObject("info").getMap());

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

                msg.reply(JsonObject.mapFrom(new DbSchema(catalog, schemas)));
            }
        }
        catch (Throwable e) {
            msg.fail(HTTP_INTERNAL_ERROR, "Failed to collect DB schemas");
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

                msg.reply(new JsonArray(metadata.stream().map(JsonObject::mapFrom).collect(Collectors.toList())));
            }
        }
        catch (Throwable e) {
            msg.fail(HTTP_INTERNAL_ERROR, "Failed to collect DB metadata");
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
