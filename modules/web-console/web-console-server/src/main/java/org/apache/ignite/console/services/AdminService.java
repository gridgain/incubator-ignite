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
import org.apache.ignite.console.json.JsonArray;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.console.tx.TransactionManager;
import org.apache.ignite.console.web.model.SignUpRequest;
import org.apache.ignite.transactions.Transaction;
import org.springframework.stereotype.Service;

import static org.apache.ignite.console.notification.model.NotificationDescriptor.ACCOUNT_DELETED;
import static org.apache.ignite.console.notification.model.NotificationDescriptor.ADMIN_WELCOME_LETTER;

/**
 * Service to handle administrator actions.
 */
@Service
public class AdminService {
    /** */
    private final TransactionManager txMgr;

    /** */
    private final AccountsService accountsSrvc;

    /** */
    private final ConfigurationsService cfgsSrvc;

    /** */
    private final NotebooksService notebooksSrvc;

    /** */
    private final NotificationService notificationSrvc;

    /**
     * @param txMgr Transactions manager.
     * @param accountsSrvc Service to work with accounts.
     * @param cfgsSrvc Service to work with configurations.
     * @param notebooksSrvc Service to work with notebooks.
     * @param notificationSrvc Service to send notifications.
     */
    public AdminService(
        TransactionManager txMgr,
        AccountsService accountsSrvc,
        ConfigurationsService cfgsSrvc,
        NotebooksService notebooksSrvc,
        NotificationService notificationSrvc
    ) {
        this.txMgr = txMgr;
        this.accountsSrvc = accountsSrvc;
        this.cfgsSrvc = cfgsSrvc;
        this.notebooksSrvc = notebooksSrvc;
        this.notificationSrvc = notificationSrvc;
    }

    /**
     * @return List of all users.
     */
    public JsonArray list() {
        List<Account> accounts = accountsSrvc.list();

        JsonArray res = new JsonArray();

        accounts.forEach(account ->
            res.add(new JsonObject()
                .add("id", account.getId())
                .add("firstName", account.getFirstName())
                .add("lastName", account.getLastName())
                .add("admin", account.getAdmin())
                .add("email", account.getUsername())
                .add("company", account.getCompany())
                .add("country", account.getCountry())
                .add("lastLogin", account.lastLogin())
                .add("lastActivity", account.lastActivity())
                .add("activated", account.isEnabled())
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
     * Delete account by ID.
     *
     * @param accId Account ID.
     */
    public void delete(UUID accId) {
        try (Transaction tx = txMgr.txStart()) {
            cfgsSrvc.deleteByAccountId(accId);
            notebooksSrvc.deleteByAccountId(accId);
            Account acc = accountsSrvc.delete(accId);

            tx.commit();

            notificationSrvc.sendEmail(ACCOUNT_DELETED, acc);
        }
    }

    /**
     * @param accId Account ID.
     * @param admin Admin flag.
     */
    public void toggle(UUID accId, boolean admin) {
        accountsSrvc.toggle(accId, admin);
    }

    /**
     * @param accId Account ID.
     */
    public void become(UUID accId) {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    /**
     * @param params SignUp params.
     */
    public void registerUser(SignUpRequest params) {
        Account acc = accountsSvc.create(params);

        notificationSrvc.sendEmail(ADMIN_WELCOME_LETTER, acc);
    }
}
