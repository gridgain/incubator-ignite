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

package org.apache.ignite.console.web.security;

import java.io.IOException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.ignite.console.web.model.SignInRequest;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedCredentialsNotFoundException;

import static org.apache.ignite.console.util.JsonUtils.fromJson;

/**
 * Custom filter for retrieve credentials from body and authenticate user. Default implementation use path parameters.
 */
public class RequestBodyReaderAuthenticationFilter extends UsernamePasswordAuthenticationFilter {
    /** {@inheritDoc} */
    @Override public Authentication attemptAuthentication(HttpServletRequest req,
        HttpServletResponse res) throws AuthenticationException {
        try {
            SignInRequest params = fromJson(req.getReader(), SignInRequest.class);

            UsernamePasswordAuthenticationToken tok =
                new UsernamePasswordAuthenticationToken(params.getEmail(), params.getPassword());

            // Allow subclasses to set the "details" property
            setDetails(req, tok);

            return getAuthenticationManager().authenticate(tok);
        }
        catch (IOException e) {
            throw new PreAuthenticatedCredentialsNotFoundException("Failed to parse signin request", e);
        }
    }
}
