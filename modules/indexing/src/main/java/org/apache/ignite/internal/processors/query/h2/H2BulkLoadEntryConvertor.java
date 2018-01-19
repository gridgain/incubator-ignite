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

package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.bulkload.BulkLoadEntryConverter;
import org.apache.ignite.internal.processors.query.h2.dml.UpdateMode;
import org.apache.ignite.internal.processors.query.h2.dml.UpdatePlan;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.lang.IgniteBiTuple;

import java.util.List;

/** FIXME SHQ */
public class H2BulkLoadEntryConvertor implements BulkLoadEntryConverter {

    private UpdatePlan updatePlan;

    public H2BulkLoadEntryConvertor(GridH2Table table) {

    }

    @Override public IgniteBiTuple<?, ?> convertRecord(List<?> record) throws IgniteCheckedException {
        return updatePlan.processRow(record);
    }
}
