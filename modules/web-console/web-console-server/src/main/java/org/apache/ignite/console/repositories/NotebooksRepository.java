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
import org.apache.ignite.console.db.OneToManyIndex;
import org.apache.ignite.console.db.Table;
import org.apache.ignite.console.dto.Notebook;
import org.apache.ignite.console.tx.TransactionManager;
import org.apache.ignite.transactions.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

/**
 * Repository to work with notebooks.
 */
@Repository
public class NotebooksRepository extends AbstractRepository<Notebook> {
    /** */
    private static final Logger log = LoggerFactory.getLogger(NotebooksRepository.class);

    /** */
    private final Table<Notebook> notebooksTbl;

    /** */
    private final OneToManyIndex notebooksIdx;

    /**
     * @param ignite Ignite.
     * @param txMgr Transactions manager.
     */
    public NotebooksRepository(Ignite ignite, TransactionManager txMgr) {
        super(ignite, txMgr);

        notebooksTbl = new Table<Notebook>(ignite, "wc_notebooks")
            .addUniqueIndex(Notebook::getName, (notebook) -> "Notebook '" + notebook.getName() + "' already exits");

        notebooksIdx = new OneToManyIndex(ignite, "wc_account_notebooks_idx");
    }

    /**
     * @param accId Account ID.
     * @return List of notebooks for specified account.
     */
    public Collection<Notebook> list(UUID accId) {
        return loadList(accId, accId, notebooksIdx, notebooksTbl);
    }

    /**
     * Save notebook.
     *
     * @param accId Account ID.
     * @param notebook Notebook to save.
     */
    public void save(UUID accId, Notebook notebook) {
        try (Transaction tx = txStart()) {
            notebook.setAccountId(accId);
            
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
        try (Transaction tx = txStart()) {
            Notebook notebook = notebooksTbl.load(notebookId);

            if (notebook != null) {
                checkOwner(accId, notebook);

                notebooksTbl.delete(notebookId);

                notebooksIdx.remove(accId, notebookId);

                tx.commit();
            }
            else
                log.warn("Detected attempt to delete not existing notebook [accId=" + accId +
                    ", notebookId=" + notebookId + "]");
        }
    }

    /**
     * Delete all notebook for specified account.
     *
     * @param accId Account ID.
     */
    public void deleteAll(UUID accId) {
        try(Transaction tx = txStart()) {
            TreeSet<UUID> ids = notebooksIdx.delete(accId);

            notebooksTbl.deleteAll(ids);

            tx.commit();
        }
    }
}
