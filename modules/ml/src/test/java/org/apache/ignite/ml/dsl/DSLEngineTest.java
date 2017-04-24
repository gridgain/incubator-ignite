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

package org.apache.ignite.ml.dsl;

import java.util.Scanner;
import javax.script.ScriptException;
import org.junit.Test;

/**
 * TODO: add description.
 */
public class DSLEngineTest {
    @Test
    public void testSimpleJS() throws ScriptException {
        DSLEngine engine = new DSLEngine("nashorn");

        try (Scanner scanner = new Scanner(DSLEngineTest.class.getResourceAsStream("/org/apache/ignite/dsl/js/simpleJS.js"))){
            String rawScript = scanner.useDelimiter("\\A").next();

            engine.run(rawScript);
        }
    }

    @Test
    public void testSimplePython() throws ScriptException {
        DSLEngine engine = new DSLEngine("jython");

        try (Scanner scanner = new Scanner(DSLEngineTest.class.getResourceAsStream("/org/apache/ignite/dsl/js/simpleJython.py"))){
            String rawScript = scanner.useDelimiter("\\A").next();

            engine.run(rawScript);
        }
    }
}
