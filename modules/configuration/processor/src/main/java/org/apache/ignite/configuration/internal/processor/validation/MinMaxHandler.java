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

package org.apache.ignite.configuration.internal.processor.validation;

import javax.lang.model.element.VariableElement;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import org.apache.ignite.configuration.internal.validation.ConfigurationValidationException;

class MinMaxHandler implements Handler {
    /** {@inheritDoc} */
    @Override public CodeBlock generate(VariableElement variableElement, FieldSpec field) {
        String name = field.name;

        final Min minAnnotation = variableElement.getAnnotation(Min.class);
        final Max maxAnnotation = variableElement.getAnnotation(Max.class);

        final CodeBlock.Builder builder = CodeBlock.builder();

        if (minAnnotation != null) {
            final long minValue = minAnnotation.value();
            final String minValueMessage = minAnnotation.message();
            builder.beginControlFlow("if (object.$L() < $L)", name, minValue);
            builder.addStatement("throw new $T($S)", ConfigurationValidationException.class, minValueMessage);
            builder.endControlFlow();
        }

        if (maxAnnotation != null) {
            final long maxValue = maxAnnotation.value();
            final String maxValueMessage = maxAnnotation.message();
            builder.beginControlFlow("if (object.$L() > $L)", name, maxValue);
            builder.addStatement("throw new $T($S)", ConfigurationValidationException.class, maxValueMessage);
            builder.endControlFlow();
        }

        return builder.build();
    }

    /** {@inheritDoc} */
    @Override public boolean supports(VariableElement variableElement, FieldSpec field) {
        return variableElement.getAnnotation(Min.class) != null || variableElement.getAnnotation(Max.class) != null;
    }
}
