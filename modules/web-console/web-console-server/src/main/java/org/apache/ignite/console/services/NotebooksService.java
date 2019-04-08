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

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.repositories.NotebooksRepository;
import org.apache.ignite.console.dto.Notebook;
import org.springframework.stereotype.Service;

/**
 * Service to handle notebooks.
 */
@Service
public class NotebooksService extends AbstractService {
    /** Repository to work with notebooks. */
    private final NotebooksRepository notebooksRepo;

    /** Accounts service. */
    private AccountsService accountsSrvc;

    /**
     * @param ignite Ignite.
     */
    public NotebooksService(Ignite ignite, AccountsService accountsSrvc) {
        super(ignite);

        this.notebooksRepo = new NotebooksRepository(ignite);
        this.accountsSrvc = accountsSrvc;
    }

    /**
     * Delete all notebook for specified user.
     *
     * @param accId Account ID.
     */
    void deleteByAccountId(UUID accId) {
        notebooksRepo.deleteByAccount(accId);
    }

    /**
     * @param email Account email.
     * @return List of user notebooks.
     */
    public Collection<Notebook> load(String email) {
        Account acc = accountsSrvc.loadUserByUsername(email);

        return notebooksRepo.list(acc.getId());
    }

    /**
     * @param email Account email.
     * @param notebook Notebook.
     */
    public void save(String email, Notebook notebook) {
        Account acc = accountsSrvc.loadUserByUsername(email);

        notebooksRepo.save(acc.getId(), notebook);
    }

    /**
     * @param email Account email.
     * @param notebookId Notebook id.
     */
    public void delete(String email, UUID notebookId) {
        Account acc = accountsSrvc.loadUserByUsername(email);

        notebooksRepo.delete(acc.getId(), notebookId);
    }
}
