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

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.db.Schemas;
import org.apache.ignite.console.dto.Notebook;
import org.apache.ignite.console.json.JsonArray;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.console.repositories.NotebooksRepository;

import static org.apache.ignite.console.common.Utils.toJsonArray;

/**
 * Service to handle notebooks.
 */
public class NotebooksService extends AbstractService {
    /** Repository to work with notebooks. */
    private final NotebooksRepository notebooksRepo;

    /**
     * @param ignite Ignite.
     */
    public NotebooksService(Ignite ignite) {
        super(ignite);

        this.notebooksRepo = new NotebooksRepository(ignite);
    }

    /** {@inheritDoc} */
    @Override public NotebooksService install() {
//        addConsumer(vertx, Addresses.NOTEBOOK_LIST, this::load);
//        addConsumer(vertx, Addresses.NOTEBOOK_SAVE, this::save);
//        addConsumer(vertx, Addresses.NOTEBOOK_DELETE, this::delete);

        return this;
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
     * @param params Parameters in JSON format.
     * @return List of user notebooks.
     */
    private JsonArray load(JsonObject params) {
        UUID userId = getUserId(params);

        return toJsonArray(notebooksRepo.list(userId));
    }

    /**
     * @param params Parameters in JSON format.
     * @return Affected rows JSON object.
     */
    private JsonObject save(JsonObject params) {
        UUID userId = getUserId(params);
        Notebook notebook = Notebook.fromJson(Schemas.sanitize(Notebook.class, getProperty(params, "notebook")));

        notebooksRepo.save(userId, notebook);

        return rowsAffected(1);
    }

    /**
     * @param params Parameters in JSON format.
     * @return Affected rows JSON object.
     */
    private JsonObject delete(JsonObject params) {
        UUID userId = getUserId(params);
        UUID notebookId = getId(getProperty(params, "notebook"));

        int rmvCnt = notebooksRepo.delete(userId, notebookId);

        return rowsAffected(rmvCnt);
    }
}
