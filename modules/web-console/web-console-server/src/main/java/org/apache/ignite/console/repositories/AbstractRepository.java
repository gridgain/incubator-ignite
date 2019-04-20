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
import org.apache.ignite.console.db.OneToManyIndex;
import org.apache.ignite.console.db.Table;
import org.apache.ignite.console.dto.AbstractDto;
import org.apache.ignite.console.services.OwnerValidationService;
import org.apache.ignite.console.tx.TransactionManager;
import org.apache.ignite.transactions.Transaction;

/**
 * Repository to work with notebooks.
 */
public abstract class AbstractRepository<T extends AbstractDto>  {
    /** */
    protected final TransactionManager txMgr;

    /** */
    protected final OwnerValidationService ownerValidationSrvc;

    /**
     * @param txMgr Tran.
     */
    protected AbstractRepository(
        TransactionManager txMgr,
        OwnerValidationService ownerValidationSrvc
    ) {
        this.txMgr = txMgr;
        this.ownerValidationSrvc = ownerValidationSrvc;
    }

    /**
     * Start transaction.
     *
     * @return Transaction.
     */
    public Transaction txStart() {
        return txMgr.txStart();
    }

    /**
     * Load short list of DTOs.
     *
     * @param accId Account ID.
     * @param ownerId Owner ID.
     * @param ownerIdx Index with DTOs IDs.
     * @param tbl Table with DTOs.
     */
    protected Collection<T> loadList(
        UUID accId,
        UUID ownerId,
        OneToManyIndex ownerIdx,
        Table<T> tbl
    ) {
        Collection<T> dtos;

        try (Transaction ignored = txStart()) {
            TreeSet<UUID> ids = ownerIdx.load(ownerId);

            dtos = tbl.loadAll(ids);
        }

        checkOwner(accId, dtos);

        return dtos;
    }

    /**
     * @param accId Account ID.
     * @param dto Object ID.
     */
    protected void registerOwner(UUID accId, AbstractDto dto) {
        ownerValidationSrvc.register(accId, dto.getId());
    }

    /**
     * @param accId Expected account ID.
     * @param dto Object to check.
     * @throws SecurityException If detected illegal access.
     */
    protected void checkOwner(UUID accId, AbstractDto dto) throws SecurityException {
        ownerValidationSrvc.validate(accId, dto.getId());
//        if (!accId.equals(dto.getAccountId()))
//            throw new SecurityException("Illegal access to other user data");
    }

    /**
     * @param accId Expected account ID.
     * @param dtos Objects to check.
     * @throws SecurityException If detected illegal access.
     */
    protected void checkOwner(UUID accId, Collection<? extends AbstractDto> dtos) throws SecurityException {
        dtos.forEach(dto -> checkOwner(accId, dto));
    }
}
