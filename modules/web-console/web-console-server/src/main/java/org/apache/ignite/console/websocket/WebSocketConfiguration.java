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

package org.apache.ignite.console.websocket;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistration;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

import static org.apache.ignite.console.websocket.WebSocketConsts.AGENTS_PATH;
import static org.apache.ignite.console.websocket.WebSocketConsts.BROWSERS_PATH;

/**
 * Websocket configuration.
 */
@Configuration
@EnableWebSocket
public class WebSocketConfiguration implements WebSocketConfigurer {
	/** */
	private final WebSocketSessions wss;

	/** */
	@Value("${websocket.ssl.enabled}")
	private boolean sslEnabled;

	/** */
	@Value("${websocket.allowed.origin}")
	private String allowedOrigin;

	/**
	 * @param wss Websocket sessions.
	 */
	@Autowired
	public WebSocketConfiguration(WebSocketSessions wss) {
		this.wss = wss;
	}

	/**
	 * @param registry Registry.
	 */
	@Override public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
		WebSocketHandlerRegistration reg = registry.addHandler(new WebSocketRouter(wss), AGENTS_PATH, BROWSERS_PATH);

		if (sslEnabled)
			reg.setAllowedOrigins(allowedOrigin);
	}
}
