/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements. See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.ignite.internal.processors.security;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.jetbrains.annotations.Nullable;

/**
 * Test permission set.
 */
public class TestSecurityPermissionSet implements SecurityPermissionSet {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** */
    private boolean defaultAllowAll;

    /** Task permissions. */
    private final Map<String, Collection<SecurityPermission>> taskPerms = new HashMap<>();

    /** Cache permissions. */
    private final Map<String, Collection<SecurityPermission>> cachePerms = new HashMap<>();

    /** System permissions. */
    private final List<SecurityPermission> sysPerms = new ArrayList<>();

    /** {@inheritDoc} */
    @Override public boolean defaultAllowAll() {
        return defaultAllowAll;
    }

    /** {@inheritDoc} */
    @Override public Map<String, Collection<SecurityPermission>> taskPermissions() {
        return taskPerms;
    }

    /** {@inheritDoc} */
    @Override public Map<String, Collection<SecurityPermission>> cachePermissions() {
        return cachePerms;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Collection<SecurityPermission> systemPermissions() {
        return sysPerms;
    }
}
