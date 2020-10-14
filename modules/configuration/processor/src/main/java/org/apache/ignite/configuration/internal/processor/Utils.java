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

package org.apache.ignite.configuration.internal.processor;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.VariableElement;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import org.apache.ignite.configuration.internal.DynamicConfiguration;
import org.apache.ignite.configuration.internal.NamedListConfiguration;

public class Utils {

    public static MethodSpec createConstructor(List<FieldSpec> fieldSpecs) {
        final MethodSpec.Builder builder = MethodSpec.constructorBuilder();
        fieldSpecs.forEach(field -> {
            builder.addParameter(field.type, field.name);
            builder.addStatement("this.$L = $L", field.name, field.name);
        });
        return builder.build();
    }

    public static List<MethodSpec> createGetters(List<FieldSpec> fieldSpecs) {
        return fieldSpecs.stream().map(field -> {
           return MethodSpec.methodBuilder(field.name)
            .returns(field.type)
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .addStatement("return $L", field.name)
            .build();
        }).collect(Collectors.toList());
    }

    public static List<MethodSpec> createBuildSetters(List<FieldSpec> fieldSpecs) {
        return fieldSpecs.stream().map(field -> {
            return MethodSpec.methodBuilder("with" + field.name)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addStatement("this.$L = $L", field.name, field.name)
                .build();
        }).collect(Collectors.toList());
    }

    public static CodeBlock newObject(TypeName type, List<VariableElement> fieldSpecs) {
        String args = fieldSpecs.stream().map(f -> f.getSimpleName().toString()).collect(Collectors.joining(", "));
        return CodeBlock.builder()
            .add("new $T($L)", type, args)
            .build();
    }

    public static ParameterizedTypeName getParameterized(ClassName clz, TypeName... types) {
        types = Arrays.stream(types).map(t -> {
            if (t.isPrimitive())
                t = t.box();
            return t;
        }).toArray(TypeName[]::new);
        return ParameterizedTypeName.get(clz, types);
    }

    public static ClassName getConfigurationName(ClassName clz) {
        return ClassName.get(
            clz.packageName(),
            clz.simpleName().replace("Schema", "")
        );
    }

    public static ClassName getViewName(ClassName clz) {
        return ClassName.get(
            clz.packageName(),
            clz.simpleName().replace("ConfigurationSchema", "")
        );
    }

    public static ClassName getInitName(ClassName clz) {
        return ClassName.get(
            clz.packageName(),
            "Init" + clz.simpleName().replace("ConfigurationSchema", "")
        );
    }

    public static ClassName getChangeName(ClassName clz) {
        return ClassName.get(
            clz.packageName(),
            "Change" + clz.simpleName().replace("ConfigurationSchema", "")
        );
    }

    public static boolean isNamedConfiguration(TypeName type) {
        if (type instanceof ParameterizedTypeName) {
            ParameterizedTypeName parameterizedTypeName = (ParameterizedTypeName) type;

            if (parameterizedTypeName.rawType.equals(ClassName.get(NamedListConfiguration.class)))
                return true;
        }
        return false;
    }

    public static TypeName unwrapConfigurationClass(TypeName type) {
        if (type instanceof ParameterizedTypeName) {
            ParameterizedTypeName parameterizedTypeName = (ParameterizedTypeName) type;

            if (parameterizedTypeName.rawType.equals(ClassName.get(NamedListConfiguration.class)))
                return parameterizedTypeName.typeArguments.get(1);

            if (parameterizedTypeName.rawType.equals(ClassName.get(DynamicConfiguration.class)))
                return parameterizedTypeName.typeArguments.get(0);
        }
        return type;
    }

}
