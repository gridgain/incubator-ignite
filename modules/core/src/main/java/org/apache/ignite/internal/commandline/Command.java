/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.commandline;

/**
 * Command.
 */
public enum Command {
    /** */
    ACTIVATE("--activate"),

    /** */
    DEACTIVATE("--deactivate"),

    /** */
    STATE("--state"),

    /** */
    BASELINE("--baseline"),

    /** */
    TX("--tx"),

    /** */
    CACHE("--cache"),

    /** */
    WAL("--wal"),

    /** */
    DIAGNOSTIC("--diagnostic"),
    ;

    /** Private values copy so there's no need in cloning it every time. */
    private static final Command[] VALUES = Command.values();

    /** */
    private final String text;

    /**
     * @param text Text.
     */
    Command(String text) {
        this.text = text;
    }

    /**
     * @param text Command text.
     * @return Command for the text.
     */
    public static Command of(String text) {
        for (Command cmd : VALUES) {
            if (cmd.text().equalsIgnoreCase(text))
                return cmd;
        }

        return null;
    }

    /**
     * @return Command text.
     */
    public String text() {
        return text;
    }

    /** {@inheritDoc} */
    @Override public String toString() { 
        return text; 
    }
}
