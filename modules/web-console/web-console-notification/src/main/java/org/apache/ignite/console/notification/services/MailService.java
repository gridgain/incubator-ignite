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

package org.apache.ignite.console.notification.services;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;
import org.apache.ignite.console.model.Notification;
import org.apache.ignite.console.model.NotificationType;
import org.apache.ignite.console.notification.config.MessagesProperties;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.springframework.core.io.ClassPathResource;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.common.TemplateParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.mail.MailException;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;

@Service
public class MailService {
    /** Expression parser. */
    private final ExpressionParser parser = new SpelExpressionParser();

    /** Template parser context. */
    private final TemplateParserContext templateParserCtx = new TemplateParserContext("${", "}");

    /** JavaMail sender */
    private JavaMailSender mailSnd;

    /** Mail config. */
    private MessagesProperties cfg;

    /**
     * @param mailSnd Mail sender.
     * @param cfg Mail properties.
     *
     */
    public MailService(JavaMailSender mailSnd, MessagesProperties cfg) {
        this.mailSnd = mailSnd;
        this.cfg = cfg;
    }

    /**
     * Send email.
     *
     * @param notification Notification.
     */
    public void send(Notification notification) {
        try {
            MimeMessage msg = mailSnd.createMimeMessage();

            MimeMessageHelper msgHelper = new MimeMessageHelper(msg);

            msgHelper.setTo(notification.getRecipient().getEmail());

            EvaluationContext ctx = createContext(notification);

            String sbj = processExpressions(notification.getSubject(), ctx);

            msgHelper.setSubject(sbj);

            String template = loadMessageTemplate(notification.getType());
            
            msgHelper.setText(processExpressions(template == null ? notification.getMessage() : template, ctx), true);

            mailSnd.send(msg);
        }
        catch (MessagingException | MailException | IOException e) {

        }
    }

    /**
     * @param notification Notification.
     */
    private EvaluationContext createContext(Notification notification) {
        return new StandardEvaluationContext(notification);
    }

    /**
     * @param type Notification type.
     */
    private String loadMessageTemplate(NotificationType type) throws IOException {
        String path = cfg.getTemplatePath(type);

        if (path == null)
            return null;

        URL url;

        try {
            url = new URL(path);
        }
        catch (MalformedURLException e) {
            url = U.resolveIgniteUrl(path);

            if (url == null)
                url = new ClassPathResource(path).getURL();
        }

        try (Scanner scanner = new Scanner(url.openStream(), StandardCharsets.UTF_8.toString())) {
            scanner.useDelimiter("\\A");

            return scanner.hasNext() ? scanner.next() : "";
        }
    }

    /**
     * @param template Template.
     * @param ctx Context.
     */
    private String processExpressions(String template, EvaluationContext ctx) {
        return parser.parseExpression(template, templateParserCtx).getValue(ctx, String.class);
    }
}
