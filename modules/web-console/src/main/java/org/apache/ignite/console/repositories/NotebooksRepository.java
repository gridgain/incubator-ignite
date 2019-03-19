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
import org.apache.ignite.console.dto.DataObject;
import org.apache.ignite.console.dto.Notebook;
import org.apache.ignite.transactions.Transaction;

/**
 * Repository to work with notebooks.
 */
public class NotebooksRepository extends AbstractRepository {
    /** */
    private final Table<Notebook> notebooksTbl;

    /** */
    private final OneToManyIndex notebooksIdx;

    /**
     * @param ignite Ignite.
     */
    public NotebooksRepository(Ignite ignite) {
        super(ignite);

        notebooksTbl = new Table<Notebook>(ignite, "wc_notebooks")
            .addUniqueIndex(Notebook::name, (notebook) -> "Notebook '" + notebook.name() + "' already exits");

        notebooksIdx = new OneToManyIndex(ignite, "wc_account_notebooks_idx");
    }

    /** {@inheritDoc} */
    @Override protected void initDatabase() {
        notebooksTbl.cache();
        notebooksIdx.cache();
    }

    /**
     * @param userId User ID.
     * @return List of user notebooks.
     */
    public Collection<? extends DataObject> list(UUID userId) {
        return loadList(userId, notebooksIdx, notebooksTbl);
    }

    /**
     * Save notebook.
     *
     * @param userId User ID.
     * @param notebook Notebook to save.
     */
    public void save(UUID userId, Notebook notebook) {
        try (Transaction tx = txStart()) {
            notebooksTbl.save(notebook);

            notebooksIdx.add(userId, notebook.id());

            tx.commit();
        }
    }

    /**
     * Delete notebook.
     *
     * @param userId User ID.
     * @param notebookId Notebook ID to delete.
     * @return Number of removed notebooks.
     */
    public int delete(UUID userId, UUID notebookId) {
        int rmvCnt = 0;

        try (Transaction tx = txStart()) {
            Notebook notebook = notebooksTbl.delete(notebookId);

            if (notebook != null) {
                notebooksIdx.remove(userId, notebookId);

                rmvCnt = 1;
            }

            tx.commit();
        }

        return rmvCnt;
    }

    /**
     * Delete all notebook for specified user.
     *
     * @param accId Account ID.
     */
    public void deleteByAccount(UUID accId) {
        try(Transaction tx = txStart()) {
            TreeSet<UUID> ids = notebooksIdx.delete(accId);

            notebooksTbl.deleteAll(ids);

            tx.commit();
        }
    }
}
