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

/** Describes queryable fields defined in {@link QueryEntity}. */
public final class QueryField {
    /** Name. */
    private String name;

    /** Type name. */
    private String typeName;

    /** Is Key field flag. */
    private boolean keyField;

    /** Is Not null flag. */
    private boolean notNull;

    /** Default value. */
    private Object dfltVal;

    /** Constructor. */
    public QueryField(String name, String typeName) {
        this.name = name;
        this.typeName = typeName;
    }

    /** @return Name. */
    public String getName() {
        return name;
    }

    /** @param name Name. */
    public QueryField setName(String name) {
        this.name = name;

        return this;
    }

    /** @return Type name. */
    public String getTypeName() {
        return typeName;
    }

    /** @param typeName  Type name. */
    public QueryField setTypeName(String typeName) {
        this.typeName = typeName;

        return this;
    }

    /** @return Is key field flag. */
    public boolean isKeyField() {
        return keyField;
    }

    /** @param keyField  Is key field flag. */
    public QueryField setKeyField(boolean keyField) {
        this.keyField = keyField;

        return this;
    }

    /** @return Is not null flag. */
    public boolean isNotNull() {
        return notNull;
    }

    /** @param notNull Is not null flag. */
    public QueryField setNotNull(boolean notNull) {
        this.notNull = notNull;

        return this;
    }

    /** @return Default value. */
    public Object getDefaultValue() {
        return dfltVal;
    }

    /** @param dfltVal  Default value. */
    public QueryField setDefaultValue(Object dfltVal) {
        this.dfltVal = dfltVal;

        return this;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (!(obj instanceof QueryField))
            return false;

        QueryField other = (QueryField)obj;

        return Objects.equals(name, other.name) &&
            Objects.equals(typeName, other.typeName) &&
            keyField == other.keyField &&
            notNull == other.notNull &&
            Objects.equals(dfltVal, other.dfltVal);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(name, typeName, keyField, notNull, dfltVal);
    }
}
