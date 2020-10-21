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

package org.apache.ignite.configuration.internal.selector;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BaseSelectors {

    private static Map<String, SelectorHolder> selectors = new HashMap<>();

    public static AnotherSelector find(String name) {
        String[] splitten = name.split("\\.");
        List<String> arguments = new ArrayList<>();
        StringBuilder keyBuilder = new StringBuilder();
        for (int i = 0; i < splitten.length; i++) {
            String part = splitten[i];
            int start = part.indexOf('[');
            String methodArg = null;
            if (start != -1) {
                int end = part.indexOf(']');
                if (end != -1) {
                    methodArg = part.substring(start + 1, end);
                    part = part.substring(0, start);
                }
            }
            if (methodArg != null)
                arguments.add(methodArg);

            keyBuilder.append(part);

            if (i != splitten.length - 1)
                keyBuilder.append('.');
        }
        final SelectorHolder selector = selectors.get(keyBuilder.toString());
        try {
            return selector.get(arguments);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        return null;
    }

    public static void put(String key, AnotherSelector<?, ?, ?, ?, ?> selector) {
        selectors.put(key, new SelectorHolder(selector));
    }

    public static void put(String key, MethodHandle handle) {
        selectors.put(key, new SelectorHolder(handle));
    }

    private static final class SelectorHolder {

        AnotherSelector<?, ?, ?, ?, ?> selector;

        MethodHandle selectorFn;

        public SelectorHolder(AnotherSelector<?, ?, ?, ?, ?> selector) {
            this.selector = selector;
        }

        public SelectorHolder(MethodHandle selectorFn) {
            this.selectorFn = selectorFn;
        }

        AnotherSelector<?, ?, ?, ?, ?> get(List<String> arguments) throws Throwable {
            if (selector != null)
                return selector;

            return (AnotherSelector<?, ?, ?, ?, ?>) selectorFn.invokeWithArguments(arguments);
        }

    }

}
