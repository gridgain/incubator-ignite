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

package org.apache.ignite.console.notification.config;

import java.util.Map;
import org.apache.ignite.console.notification.model.NotificationDescriptor;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Mail configuration.
 */
@Component
@ConfigurationProperties(prefix = "spring.mail.templates")
public class MessagesProperties {
    private String defaultTemplatePath;

    /** Templates path. */
    private Map<NotificationDescriptor, String> templates;

    public String getDefaultTemplatePath() {
        return defaultTemplatePath;
    }

    public void setDefaultTemplatePath(String defaultTemplatePath) {
        this.defaultTemplatePath = defaultTemplatePath;
    }

    /**
     * @param type Notification type.
     */
    public String getTemplatePath(NotificationDescriptor type) {
        return templates == null ? defaultTemplatePath : templates.getOrDefault(type, defaultTemplatePath);
    }

    /**
     * @return Templates path.
     */
    public Map<NotificationDescriptor, String> getTemplates() {
        return templates;
    }

    /**
     * @param templates New templates path.
     */
    public void setTemplates(Map<NotificationDescriptor, String> templates) {
        this.templates = templates;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MessagesProperties.class, this);
    }
}
