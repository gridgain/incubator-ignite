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
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.common.Addresses;
import org.apache.ignite.console.db.OneToManyIndex;
import org.apache.ignite.console.db.Schemas;
import org.apache.ignite.console.db.Table;
import org.apache.ignite.console.dto.DataObject;
import org.apache.ignite.console.dto.Notebook;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.console.common.Utils.toJsonArray;

/**
 * Service to handle notebooks.
 */
public class NotebooksService extends AbstractService {
    /** */
    private final Table<Notebook> notebooksTbl;

    /** */
    private final OneToManyIndex notebooksIdx;

    /**
     * @param ignite Ignite.
     */
    public NotebooksService(Ignite ignite) {
        super(ignite);

        notebooksTbl = new Table<Notebook>(ignite, "wc_notebooks")
            .addUniqueIndex(Notebook::name, (notebook) -> "Notebook '" + notebook.name() + "' already exits");

        notebooksIdx = new OneToManyIndex(ignite, "wc_account_notebooks_idx");
    }

    /** {@inheritDoc} */
    @Override protected void initialize() {
        notebooksTbl.cache();
        notebooksIdx.cache();
    }

    /** {@inheritDoc} */
    @Override public void start() {
        EventBus eventBus = vertx.eventBus();

        eventBus.consumer(Addresses.NOTEBOOK_LIST, this::load);
        eventBus.consumer(Addresses.NOTEBOOK_SAVE, this::save);
        eventBus.consumer(Addresses.NOTEBOOK_DELETE, this::delete);
    }

    /**
     * @param msg Message to process.
     */
    private void load(Message<JsonObject> msg) {
        vertx.executeBlocking(
            fut -> {
                try {
                    UUID userId = getUserId(msg);

                    Collection<? extends DataObject> notebooks = loadList(userId, notebooksIdx, notebooksTbl);

                    fut.complete(toJsonArray(notebooks));
                }
                catch (Throwable e) {
                    fut.fail(e);
                }
            },
            new ReplyHandler(msg)
        );
    }

    /**
     * @param msg Message to process.
     */
    private void save(Message<JsonObject> msg) {
        vertx.executeBlocking(
            fut -> {
                try {
                    UUID userId = getUserId(msg);
                    Notebook notebook = Notebook.fromJson(Schemas.sanitize(Notebook.class, getProperty(msg, "notebook")));

                    try (Transaction tx = txStart()) {
                        notebooksTbl.save(notebook);

                        notebooksIdx.add(userId, notebook.id());

                        tx.commit();
                    }

                    fut.complete();
                }
                catch (Throwable e) {
                    fut.fail(e);
                }
            },
            new ReplyHandler(msg)
        );
    }

    /**
     * @param msg Message to process.
     */
    private void delete(Message<JsonObject> msg) {
        vertx.executeBlocking(
            fut -> {
                try {
                    UUID userId = getUserId(msg);
                    UUID notebookId = getId(getProperty(msg, "notebook"));

                    int rmvCnt = 0;

                    try (Transaction tx = txStart()) {
                        Notebook notebook = notebooksTbl.delete(notebookId);

                        if (notebook != null) {
                            notebooksIdx.remove(userId, notebookId);

                            rmvCnt = 1;
                        }

                        tx.commit();
                    }

                    fut.complete(rowsAffected(rmvCnt));
                }
                catch (Throwable e) {
                    fut.fail(e);
                }
            },
            new ReplyHandler(msg)
        );
    }
}
