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

package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.maintenance.MaintenanceAction;

import java.io.File;

/**
 *
 */
public class CleanCacheStoresMaintenanceAction implements MaintenanceAction {
    /** */
    private final File rootStoreDir;

    /** */
    private final String[] cacheStoreDirs;

    /**
     * @param rootStoreDir
     * @param cacheStoreDirs
     */
    public CleanCacheStoresMaintenanceAction(File rootStoreDir, String[] cacheStoreDirs) {
        this.rootStoreDir = rootStoreDir;
        this.cacheStoreDirs = cacheStoreDirs;
    }

    /** {@inheritDoc} */
    @Override public void execute() {
        for (String cacheStoreDirName : cacheStoreDirs) {
            File cacheStoreDir = new File(rootStoreDir, cacheStoreDirName);

            if (cacheStoreDir.exists() && cacheStoreDir.isDirectory()) {
                for (File file : cacheStoreDir.listFiles())
                    file.delete();
            }
        }
    }
}
