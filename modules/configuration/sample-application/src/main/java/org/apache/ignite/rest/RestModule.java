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

package org.apache.ignite.rest;

import java.io.StringReader;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import io.javalin.Javalin;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.configuration.extended.ChangeLocal;
import org.apache.ignite.configuration.extended.Local;
import org.apache.ignite.configuration.extended.LocalConfiguration;
import org.apache.ignite.configuration.extended.Selectors;
import org.apache.ignite.configuration.internal.Configurator;
import org.apache.ignite.configuration.internal.selector.SelectorNotFoundException;
import org.apache.ignite.configuration.internal.validation.ConfigurationValidationException;

/** */
public class RestModule {
    /** */
    private static final String CONF_URL = "/management/v1/configuration/";

    /** */
    private static final String PATH_PARAM = "selector";

    /** */
    private final ConfigurationModule confModule;

    /** */
    public RestModule(ConfigurationModule confModule) {
        this.confModule = confModule;
    }

    /** */
    public void start() {
        Configurator<LocalConfiguration> configurator = confModule.localConfigurator();

        Javalin app = Javalin.create().start(8080);

        Gson gson = new Gson();

        app.get(CONF_URL, ctx -> {
            Local local = configurator.getRoot().toView();

            ctx.result(gson.toJson(local));
        });

        app.get(CONF_URL + ":" + PATH_PARAM, ctx -> {
            try {
                String selector = ctx.pathParam(PATH_PARAM);

                Object subTree = configurator.getPublic(Selectors.find(selector));

                String res = gson.toJson(subTree);

                ctx.result(res);
            }
            catch (SelectorNotFoundException selectorE) {
                ErrorResult eRes = new ErrorResult("CONFIG_PATH_UNRECOGNIZED", selectorE.getMessage());

                ctx.status(400).result(gson.toJson(new ResponseWrapper(eRes)));
            }
        });

        app.post(CONF_URL, ctx -> {
            try {
                StringReader strReader = new StringReader(ctx.body());

                ChangeLocalWrapper local = gson.fromJson(strReader, ChangeLocalWrapper.class);

                configurator.set(Selectors.LOCAL, local.local);
            }
            catch (SelectorNotFoundException selectorE) {
                ErrorResult eRes = new ErrorResult("CONFIG_PATH_UNRECOGNIZED", selectorE.getMessage());

                ctx.status(400).result(gson.toJson(new ResponseWrapper(eRes)));
            }
            catch (ConfigurationValidationException validationE) {
                ErrorResult eRes = new ErrorResult("VALIDATION_EXCEPTION", validationE.getMessage());

                ctx.status(400).result(gson.toJson(new ResponseWrapper(eRes)));
            }
            catch (Exception e) {
                ErrorResult eRes = new ErrorResult("GENERAL_ERROR", e.getMessage());

                ctx.status(400).result(gson.toJson(new ResponseWrapper(eRes)));
            }
        });
    }

    /** */
    private static class ResponseWrapper {
        /** */
        @SerializedName("error")
        private final ErrorResult res;

        /** */
        private ResponseWrapper(ErrorResult res) {
            this.res = res;
        }
    }

    /** */
    private static class ChangeLocalWrapper {
        /** */
        private ChangeLocal local;
    }
}
