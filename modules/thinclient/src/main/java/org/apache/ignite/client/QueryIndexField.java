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

package org.apache.ignite.client;

import java.util.Objects;

/** Item of a composite key defined in {@link QueryIndex#getFields()}. */
public final class QueryIndexField {
    /** Field name. */
    private String name;

    /** Sort order. */
    private boolean isDescending;

    /** Initializes {@link QueryIndexField} with the specified sort order. */
    public QueryIndexField(String name, boolean isDescending) {
        if (name == null || name.length() == 0)
            throw new IllegalArgumentException("name");

        this.name = name;
        this.isDescending = isDescending;
    }

    /** Initializes {@link QueryIndexField} with ascending sort order. */
    public QueryIndexField(String name) {
        this(name, false);
    }

    /** @return Field name. */
    public String getName() {
        return name;
    }

    /** @param name Field name. */
    public QueryIndexField setName(String name) {
        if (name == null || name.length() == 0)
            throw new IllegalArgumentException("name");

        this.name = name;

        return this;
    }

    /** @return Sort order. */
    public boolean isDescending() {
        return isDescending;
    }

    /** @param descending Sort order. */
    public QueryIndexField setDescending(boolean descending) {
        isDescending = descending;

        return this;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (!(obj instanceof QueryIndexField))
            return false;

        QueryIndexField other = (QueryIndexField)obj;

        return Objects.equals(name, other.name) && isDescending == other.isDescending;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 11;

        if (name != null)
            res = 31 * res + name.hashCode();

        res = 31 * res + (isDescending ? 1 : 0);

        return res;
    }
}
