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

package org.apache.ignite.console.services;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.tx.TransactionManager;
import org.apache.ignite.console.json.JsonArray;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.transactions.Transaction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static org.apache.ignite.console.notification.model.NotificationDescriptor.ACCOUNT_DELETED;

/**
 * Service to handle administrator actions.
 */
@Service
public class AdminService {
    /** */
    private final TransactionManager txMgr;

    /** */
    private final AccountsService accountsSvc;

    /** */
    private final ConfigurationsService cfgsSvc;

    /** */
    private final NotebooksService notebooksSvc;

    /** */
    private final NotificationService notificationSrvc;

    /**
     * @param txMgr Transactions manager.
     * @param accountsSvc Service to work with accounts.
     * @param cfgsSvc Service to work with configurations.
     * @param notebooksSvc Service to work with notebooks.
     */
    @Autowired
    public AdminService(
        TransactionManager txMgr,
        AccountsService accountsSvc,
        ConfigurationsService cfgsSvc,
        NotebooksService notebooksSvc,
        NotificationService notificationSrvc
    ) {
        this.txMgr = txMgr;
        this.accountsSvc = accountsSvc;
        this.cfgsSvc = cfgsSvc;
        this.notebooksSvc = notebooksSvc;
        this.notificationSrvc = notificationSrvc;
    }

    /**
     * @return List of all users.
     */
    public JsonArray list() {
        List<Account> accounts = accountsSvc.list();

        JsonArray res = new JsonArray();

        accounts.forEach(account ->
            res.add(new JsonObject()
                .add("id", account.getId())
                .add("getFirstName", account.getFirstName())
                .add("getLastName", account.getLastName())
                .add("admin", account.getAdmin())
                .add("email", account.getUsername())
                .add("company", account.getCompany())
                .add("country", account.getCountry())
                .add("lastLogin", account.lastLogin())
                .add("lastActivity", account.lastActivity())
                .add("activated", account.activated())
                .add("counters", new JsonObject()
                    .add("clusters", 0)
                    .add("caches", 0)
                    .add("models", 0)
                    .add("igfs", 0))
            )
        );

        return res;
    }

    /**
     * Remove account.
     *
     * @param accId Account ID.
     */
    public void delete(UUID accId) {
        try (Transaction tx = txMgr.txStart()) {
            cfgsSvc.deleteByAccountId(accId);
            notebooksSvc.deleteByAccountId(accId);
            Account acc = accountsSvc.delete(accId);

            tx.commit();

            notificationSrvc.sendEmail(ACCOUNT_DELETED, acc);
        }
    }

    /**
     * @param accId Account ID.
     * @param admin Admin flag.
     */
    public void toggle(UUID accId, boolean admin) {
        accountsSvc.toggle(accId, admin);
    }

    /**
     * @param accId Account ID.
     */
    public void become(UUID accId) {
        throw new UnsupportedOperationException("Not implemented yet!");
    }
}
