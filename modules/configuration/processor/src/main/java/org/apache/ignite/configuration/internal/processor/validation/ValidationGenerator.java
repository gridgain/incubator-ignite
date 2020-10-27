/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.VariableElement;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import javax.lang.model.type.MirroredTypeException;
import javax.lang.model.type.TypeMirror;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import org.apache.ignite.configuration.internal.annotation.Validate;
import org.apache.ignite.configuration.internal.processor.pojo.FieldMapping;
import org.apache.ignite.configuration.internal.validation.MaxValidator;
import org.apache.ignite.configuration.internal.validation.MinValidator;
import org.apache.ignite.configuration.internal.validation.NotNullValidator;
import org.apache.ignite.configuration.internal.validation.Validator;

public class ValidationGenerator {

    private List<Handler> handlers = new ArrayList<>();

    public ValidationGenerator() {
        handlers.add(new MinMaxHandler());
        handlers.add(new NotNullHandler());
    }

    public MethodSpec generateValidateMethod(TypeName type, List<FieldMapping> mappings) {
        final List<CodeBlock> blocks = mappings.stream().flatMap(mapping -> {
            final VariableElement variableElement = mapping.getVariableElement();
            final FieldSpec fieldSpec = mapping.getFieldSpec();
            return handlers.stream().filter(handler -> handler.supports(variableElement, fieldSpec))
                    .map(handler -> handler.generate(variableElement, fieldSpec));
        }).collect(Collectors.toList());

        final MethodSpec.Builder builder = MethodSpec.methodBuilder("validate")
            .addParameter(type, "object")
            .addModifiers(Modifier.PRIVATE, Modifier.FINAL, Modifier.STATIC);

        blocks.forEach(builder::addCode);

        return builder.build();
    }

    public CodeBlock generateValidators(VariableElement variableElement) {
        List<CodeBlock> validators = new ArrayList<>();
        final Min minAnnotation = variableElement.getAnnotation(Min.class);
        if (minAnnotation != null) {
            final long minValue = minAnnotation.value();
            final String message = minAnnotation.message();
            final CodeBlock build = CodeBlock.builder().add("new $T($L, $S)", MinValidator.class, minValue, message).build();
            validators.add(build);
        }

        final Max maxAnnotation = variableElement.getAnnotation(Max.class);
        if (maxAnnotation != null) {
            final long maxValue = maxAnnotation.value();
            final String message = maxAnnotation.message();
            final CodeBlock build = CodeBlock.builder().add("new $T($L, $S)", MaxValidator.class, maxValue, message).build();
            validators.add(build);
        }

        final NotNull notNull = variableElement.getAnnotation(NotNull.class);
        if (notNull != null) {
            final String message = notNull.message();
            final CodeBlock build = CodeBlock.builder().add("new $T($S)", NotNullValidator.class, message).build();
            validators.add(build);
        }

        final Validate validate = variableElement.getAnnotation(Validate.class);
        if (validate != null) {
            TypeMirror value = null;
            try {
                validate.value();
            } catch (MirroredTypeException e) {
                value = e.getTypeMirror();
            }
            final String message = validate.message();
            final CodeBlock build = CodeBlock.builder().add("new $T($S)", value, message).build();
            validators.add(build);
        }

        String text = validators.stream().map(v -> "$L").collect(Collectors.joining(","));

        final CodeBlock validatorsArguments = CodeBlock.builder().add(text, validators.toArray()).build();

        return CodeBlock.builder().add("$T.asList($L)", Arrays.class, validatorsArguments).build();
    }

}
