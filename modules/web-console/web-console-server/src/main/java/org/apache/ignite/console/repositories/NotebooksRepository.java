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

package org.apache.ignite.console.repositories;

import java.util.Collection;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.db.ForeignKeyIndex;
import org.apache.ignite.console.db.OneToManyIndex;
import org.apache.ignite.console.db.Table;
import org.apache.ignite.console.dto.Notebook;
import org.apache.ignite.console.tx.TransactionManager;
import org.apache.ignite.transactions.Transaction;
import org.springframework.stereotype.Repository;

/**
 * Repository to work with notebooks.
 */
@Repository
public class NotebooksRepository {
    /** */
    private final TransactionManager txMgr;

    /** */
    private final Table<Notebook> notebooksTbl;

    /** */
    private final OneToManyIndex notebooksIdx;

    /** */
    private final ForeignKeyIndex accFkIdx;

    /**
     * @param ignite Ignite.
     * @param txMgr Transactions manager.
     */
    public NotebooksRepository(Ignite ignite, TransactionManager txMgr) {
        this.txMgr = txMgr;

        notebooksTbl = new Table<Notebook>(ignite, "wc_notebooks")
            .addUniqueIndex(Notebook::getName, (notebook) -> "Notebook '" + notebook.getName() + "' already exits");

        notebooksIdx = new OneToManyIndex(ignite, "wc_account_notebooks_idx");

        accFkIdx = new ForeignKeyIndex(ignite, "wc_accounts_fk_idx");
    }

    /**
     * @param accId Account ID.
     * @return List of notebooks for specified account.
     */
    public Collection<Notebook> list(UUID accId) {
        try (Transaction ignored = txMgr.txStart()) {
            TreeSet<UUID> notebooksIds = notebooksIdx.load(accId);

            return notebooksTbl.loadAll(notebooksIds);
        }
    }

    /**
     * Save notebook.
     *
     * @param accId Account ID.
     * @param notebook Notebook to save.
     */
    public void save(UUID accId, Notebook notebook) {
        try (Transaction tx = txMgr.txStart()) {
            accFkIdx.add(accId, notebook.getId());

            notebooksTbl.save(notebook);

            notebooksIdx.add(accId, notebook.getId());

            tx.commit();
        }
    }

    /**
     * Delete notebook.
     *
     * @param accId Account ID.
     * @param notebookId Notebook ID to delete.
     */
    public void delete(UUID accId, UUID notebookId) {
        try (Transaction tx = txMgr.txStart()) {
            Notebook notebook = notebooksTbl.load(notebookId);

            if (notebook != null) {
                accFkIdx.delete(accId, notebook.getId());

                notebooksTbl.delete(notebookId);

                notebooksIdx.remove(accId, notebookId);

                tx.commit();
            }
        }
    }

    /**
     * Delete all notebook for specified account.
     *
     * @param accId Account ID.
     */
    public void deleteAll(UUID accId) {
        try(Transaction tx = txMgr.txStart()) {
            TreeSet<UUID> notebooksIds = notebooksIdx.delete(accId);

            accFkIdx.deleteAll(accId, notebooksIds);

            notebooksTbl.deleteAll(notebooksIds);

            tx.commit();
        }
    }
}
