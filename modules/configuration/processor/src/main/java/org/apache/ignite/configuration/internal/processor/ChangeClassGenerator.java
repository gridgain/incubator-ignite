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

package org.apache.ignite.configuration.internal.processor;

import com.squareup.javapoet.MethodSpec;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeMirror;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import org.apache.ignite.configuration.internal.annotation.Config;
import org.apache.ignite.configuration.internal.annotation.NamedConfig;
import org.apache.ignite.configuration.internal.property.NamedList;

public class ChangeClassGenerator extends ClassGenerator {

    public ChangeClassGenerator(ProcessingEnvironment env) {
        super(env);
    }

    @Override protected FieldSpec mapField(VariableElement field) {
        final Config configAnnotation = field.getAnnotation(Config.class);
        final NamedConfig namedConfigAnnotation = field.getAnnotation(NamedConfig.class);

        final TypeMirror type = field.asType();
        String name = field.getSimpleName().toString();

        TypeName fieldType = TypeName.get(type);
        if (namedConfigAnnotation != null || configAnnotation != null) {
            ClassName confClass = (ClassName) fieldType;
            fieldType = Utils.getChangeName(confClass);
            if (namedConfigAnnotation != null) {
                fieldType = ParameterizedTypeName.get(ClassName.get(NamedList.class), fieldType);
            }
            name = name.replace("Configuration", "");
        }

        return FieldSpec.builder(fieldType, name, Modifier.PRIVATE).build();
    }

    @Override protected MethodSpec mapMethod(FieldSpec field) {
        final String name = field.name;
        final String methodName = name.substring(0, 1).toUpperCase() + name.substring(1);
        return MethodSpec.methodBuilder("with" + methodName)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addParameter(field.type, name)
                .addStatement("this.$L = $L", name, name)
                .build();
    }

}
