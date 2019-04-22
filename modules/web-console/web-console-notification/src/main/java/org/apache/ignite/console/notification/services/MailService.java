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
import java.util.Locale;
import java.util.Scanner;
import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;
import org.apache.ignite.console.notification.config.MessagesProperties;
import org.apache.ignite.console.notification.model.Notification;
import org.apache.ignite.console.notification.model.NotificationDescriptor;
import org.apache.ignite.console.notification.model.Recipient;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.springframework.context.MessageSource;
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
    private static final ExpressionParser parser = new SpelExpressionParser();

    /** Template parser context. */
    private static final TemplateParserContext templateParserCtx = new TemplateParserContext("${", "}");

    /** Message source. */
    private MessageSource msgSrc;

    /** JavaMail sender */
    private JavaMailSender mailSnd;

    /** Mail config. */
    private MessagesProperties cfg;

    /**
     * @param msgSrc Message source.
     * @param mailSnd Mail sender.
     * @param cfg Mail properties.
     */
    public MailService(MessageSource msgSrc, JavaMailSender mailSnd, MessagesProperties cfg) {
        this.msgSrc = msgSrc;
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
            NotificationWrapper ctxObj = new NotificationWrapper(notification);

            EvaluationContext ctx = createContext(ctxObj);

            NotificationDescriptor desc = notification.getDescriptor();

            ctxObj.setSubject(processExpressions(getMessage(desc.subjectCode()), ctx));
            ctxObj.setMessage(processExpressions(getMessage(desc.messageCode()), ctx));

            String template = loadMessageTemplate(notification.getDescriptor());

            MimeMessage msg = mailSnd.createMimeMessage();

            MimeMessageHelper msgHelper = new MimeMessageHelper(msg);

            msgHelper.setTo(notification.getRecipient().getEmail());
            msgHelper.setSubject(ctxObj.getSubject());
            msgHelper.setText(template == null ? ctxObj.getMessage() : processExpressions(template, ctx), true);

            mailSnd.send(msg);
        }
        catch (MessagingException | MailException | IOException e) {

        }
    }

    /**
     * @param type Notification type.
     */
    private String loadMessageTemplate(NotificationDescriptor type) throws IOException {
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
     * @param rootObj the root object to use.
     */
    private EvaluationContext createContext(Object rootObj) {
        return new StandardEvaluationContext(rootObj);
    }

    /**
     * @param expression The raw expression string to parse.
     * @param ctx Context.
     */
    private String processExpressions(String expression, EvaluationContext ctx) {
        return parser.parseExpression(expression, templateParserCtx).getValue(ctx, String.class);
    }

    /**
     * Try to resolve the message.
     * @param code Code.
     */
    private String getMessage(String code) {
        return msgSrc.getMessage(code, null, Locale.US);
    }

    private static class NotificationWrapper extends StandardEvaluationContext {
        private String origin;
        private Recipient rcpt;
        private String subject;
        private String message;

        private NotificationWrapper(Notification notification) {
            this.origin = notification.getOrigin();
            this.rcpt = notification.getRecipient();
        }

        public String getOrigin() {
            return origin;
        }

        public Recipient getRecipient() {
            return rcpt;
        }

        public String getSubject() {
            return subject;
        }

        public void setSubject(String subject) {
            this.subject = subject;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }
    }
}
