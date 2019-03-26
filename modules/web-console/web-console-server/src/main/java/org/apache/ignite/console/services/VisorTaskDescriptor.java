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

package org.apache.ignite.console.services;

/**
     * Visor task descriptor.
     *
     * TODO IGNITE-5617 Move to separate class?
     */
    public class VisorTaskDescriptor {
        /** */
        private static final String[] EMPTY = new String[0];

        /** */
        private final String taskCls;

        /** */
        private final String[] argCls;

        /**
         * @param taskCls Visor task class.
         * @param argCls Visor task arguments classes.
         */
        public VisorTaskDescriptor(String taskCls, String[] argCls) {
            this.taskCls = taskCls;
            this.argCls = argCls != null ? argCls : EMPTY;
        }

        /**
         * @return Visor task class.
         */
        public String getTaskClass() {
            return taskCls;
        }

        /**
         * @return Visor task arguments classes.
         */
        public String[] getArgumentsClasses() {
            return argCls;
        }
    }
