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
import org.apache.ignite.console.services.AccountsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.security.authentication.dao.DaoAuthenticationProvider;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.session.SessionRegistry;
import org.springframework.security.core.session.SessionRegistryImpl;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.security.web.authentication.logout.HttpStatusReturningLogoutSuccessHandler;
import org.springframework.security.web.csrf.CookieCsrfTokenRepository;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;

/**
 * TODO IGNITE-5617 javadocs.
 */
@Configuration
@EnableWebSecurity
public class SecurityConfig extends WebSecurityConfigurerAdapter {
    /** */
    private static final String SIGN_IN_ROUT = "/api/v1/signin";

    /** */
    private static final String SIGN_UP_ROUT = "/api/v1/signup";

    /** */
    private static final String LOGOUT_ROUT = "/api/v1/logout";

    /** */
    private final AccountsService accountsSrvc;
    
    /** */
    private final PasswordEncoder encoder;

    /**
     * @param accountsSrvc User details service.
     */
    @Autowired
    public SecurityConfig(PasswordEncoder encoder, AccountsService accountsSrvc) {
        this.encoder = encoder;
        this.accountsSrvc = accountsSrvc;
    }

    /** {@inheritDoc} */
    @Override protected void configure(HttpSecurity http) throws Exception {
        http
            .csrf()
            .csrfTokenRepository(CookieCsrfTokenRepository.withHttpOnlyFalse())
            .and()
            .authorizeRequests()
            .antMatchers(SIGN_IN_ROUT, SIGN_UP_ROUT, LOGOUT_ROUT).permitAll()
            .anyRequest().authenticated()
            .and()
            .addFilterBefore(authenticationFilter(), UsernamePasswordAuthenticationFilter.class)
            .logout()
            .logoutUrl(LOGOUT_ROUT)
            .logoutSuccessHandler(new HttpStatusReturningLogoutSuccessHandler(HttpStatus.OK));
    }

    /** {@inheritDoc} */
    @Override protected void configure(AuthenticationManagerBuilder auth) {
        final DaoAuthenticationProvider authProvider = new DaoAuthenticationProvider();

        authProvider.setUserDetailsService(accountsSrvc);
        authProvider.setPasswordEncoder(encoder);


        auth.authenticationProvider(authProvider);
    }

    /**
     * @param req Request.
     * @param res Response.
     * @param authentication Authentication.
     */
    private void loginSuccessHandler(
        HttpServletRequest req,
        HttpServletResponse res,
        Authentication authentication
    ) throws IOException {
        res.setStatus(HttpServletResponse.SC_OK);

        res.getWriter().flush();
    }

    /** TODO IGNITE-5617 javadocs. */
    @Bean
    public RequestBodyReaderAuthenticationFilter authenticationFilter() throws Exception {
        RequestBodyReaderAuthenticationFilter authenticationFilter = new RequestBodyReaderAuthenticationFilter();

        authenticationFilter.setAuthenticationManager(authenticationManagerBean());
        authenticationFilter.setRequiresAuthenticationRequestMatcher(new AntPathRequestMatcher(SIGN_IN_ROUT, "POST"));
        authenticationFilter.setAuthenticationSuccessHandler(this::loginSuccessHandler);

        return authenticationFilter;
    }

    /** TODO IGNITE-5617 javadocs. */
    @Bean
    public SessionRegistry sessionRegistry() {
        return new SessionRegistryImpl();
    }
}
