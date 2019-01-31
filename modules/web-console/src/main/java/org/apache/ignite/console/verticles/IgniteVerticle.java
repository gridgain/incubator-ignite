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

import java.io.File;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import javax.naming.AuthenticationException;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.PRNG;
import org.apache.commons.codec.binary.Hex;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.console.common.Addresses;
import org.apache.ignite.console.common.Consts;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.dto.Space;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Verticle that process requests to Ignite.
 */
public class IgniteVerticle extends AbstractVerticle {
    /** */
    private Ignite ignite;

    /** */
    private List<MessageConsumer<JsonObject>> consumers = new ArrayList<>();

    /** {@inheritDoc} */
    @Override public void init(Vertx vertx, Context ctx) {
        super.init(vertx, ctx);


    }

    /** {@inheritDoc} */
    @Override public void start() {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName("Web Console backend");
        cfg.setConsistentId("web-console-backend");
        cfg.setMetricsLogFrequency(0);
        cfg.setLocalHost("127.0.0.1");

        cfg.setWorkDirectory(new File(U.getIgniteHome(), "work-web-console").getAbsolutePath());

        TcpDiscoverySpi discovery = new TcpDiscoverySpi();

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        ipFinder.setAddresses(Collections.singletonList("127.0.0.1:60800"));

        discovery.setLocalPort(60800);
        discovery.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discovery);

        DataStorageConfiguration dataStorageCfg = new DataStorageConfiguration();

        DataRegionConfiguration dataRegionCfg = new DataRegionConfiguration();

        dataRegionCfg.setPersistenceEnabled(true);

        dataStorageCfg.setDefaultDataRegionConfiguration(dataRegionCfg);

        cfg.setDataStorageConfiguration(dataStorageCfg);

        CacheConfiguration accountsCfg = new CacheConfiguration(Consts.ACCOUNTS_CACHE_NAME);
        accountsCfg.setCacheMode(REPLICATED);

        CacheConfiguration spacesCfg = new CacheConfiguration(Consts.SPACES_CACHE_NAME);
        accountsCfg.setCacheMode(REPLICATED);

        CacheConfiguration notebooksCfg = new CacheConfiguration(Consts.NOTEBOOKS_CACHE_NAME);
        accountsCfg.setCacheMode(REPLICATED);

        cfg.setCacheConfiguration(accountsCfg, spacesCfg, notebooksCfg);

        cfg.setConnectorConfiguration(null);

        ignite = Ignition.getOrStart(cfg);

        ignite.cluster().active(true);

        EventBus eventBus = vertx.eventBus();

        consumers.add(eventBus.consumer(Addresses.IGNITE_SIGN_UP, this::handleSignUp));
        consumers.add(eventBus.consumer(Addresses.IGNITE_SIGN_UP, this::handleSignIn));
    }

    /** {@inheritDoc} */
    @Override public void stop() {

    }

    /**
     * @param authInfo JSON object with authentication info.
     * @param key Key of mandatory field to check.
     * @throws AuthenticationException If mandatory field is empty.
     */
    private void checkMandatoryAuthField(JsonObject authInfo, String key) throws AuthenticationException {
        String val = authInfo.getString(key);

        if (F.isEmpty(val))
            throw new AuthenticationException("Mandatory field missing: " + key);
    }

    /**
     * @param authInfo JSON object with authentication info.
     * @throws AuthenticationException If mandatory fields are empty.
     */
    private void checkMandatoryAuthFields(JsonObject authInfo) throws AuthenticationException {
        checkMandatoryAuthField(authInfo, "email");
        checkMandatoryAuthField(authInfo, "password");
    }

    /**
     * @return Salt for hashing.
     */
    private String salt() {
        byte[] salt = new byte[32];

        rnd.nextBytes(salt);

        return Hex.encodeHexString(salt);
    }

    /**
     * TODO IGNITE-5617 Do this method in transaction?
     *
     * @param msg
     */
    private void handleSignUp(Message<JsonObject> msg) {
        JsonObject authInfo = msg.body();

        IgniteCache<String, Account> cache = ignite.cache(Consts.ACCOUNTS_CACHE_NAME);

        String email = authInfo.getString("email");

        Account account = cache.get(email);

        if (account != null)
            msg.fail(500, "Account already exists");

        account = new Account();

        account._id = UUID.randomUUID().toString();
        account.email = authInfo.getString("email");
        account.firstName = authInfo.getString("firstName");
        account.lastName = authInfo.getString("lastName");
        account.company = authInfo.getString("company");
        account.country = authInfo.getString("country");
        account.industry = authInfo.getString("industry");
        account.admin = authInfo.getBoolean("admin", false);
        account.token = UUID.randomUUID().toString(); // TODO IGNITE-5617 How to generate token?
        account.resetPasswordToken = UUID.randomUUID().toString(); // TODO IGNITE-5617 How to generate resetPasswordToken?
        account.registered = ZonedDateTime.now().toString();
        account.lastLogin = "";
        account.lastActivity = "";
        account.lastEvent = "";
        account.demoCreated = false;
        account.salt = salt();
        account.hash = computeHash(authInfo.getString("password"), account.salt);

        cache.put(email, account);

        Space space = new Space();
        space._id = UUID.randomUUID().toString();
        space.name = "Personal space";
        space.demo = false;
        space.owner = account._id;

        ignite.cache(Consts.SPACES_CACHE_NAME).put(space._id, space);

        msg.reply(account.principal());
    }

    /**
     *
     * @param msg Message with authentication info.
     */
    private void handleSignIn(Message<JsonObject> msg) {
        try {
            JsonObject authInfo = msg.body();

            checkMandatoryAuthFields(authInfo);

            String email = authInfo.getString("email");

            IgniteCache<String, Account> cache = ignite.cache(Consts.ACCOUNTS_CACHE_NAME);

            Account account = cache.get(email);

            if (account == null)
                msg.fail(500, "Invalid email or password");

            String hash = computeHash(authInfo.getString("password"), account.salt);

            if (!hash.equals(account.hash))
                throw new AuthenticationException("Invalid email or password");
        }
        catch(Throwable e) {
            msg.fail(500, e.getMessage());
        }
    }
}
