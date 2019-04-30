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

package org.apache.ignite.console.web.model;

import java.util.UUID;
import io.swagger.annotations.ApiModelProperty;
import javax.validation.constraints.NotNull;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.hibernate.validator.constraints.NotEmpty;

/**
 * Web model of toggle admin right request.
 */
public class ToggleRequest {
    /** Email. */
    @ApiModelProperty(value = "User id.", required = true)
    @NotNull
    @NotEmpty
    private UUID id;

    /** Admin flag. */
    @ApiModelProperty(value = "Admin flag.", required = true)
    private boolean admin;

    /**
     * @return Email.
     */
    public UUID getId() {
        return id;
    }

    /**
     * @param id New email.
     */
    public void setId(UUID id) {
        this.id = id;
    }

    /**
     * @return Admin flag.
     */
    public boolean isAdmin() {
        return admin;
    }

    /**
     * @param admin Admin flag.
     */
    public void setAdmin(boolean admin) {
        this.admin = admin;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ToggleRequest.class, this);
    }
}
