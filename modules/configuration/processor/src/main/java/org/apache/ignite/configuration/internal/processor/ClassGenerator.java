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

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.processing.Filer;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.VariableElement;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;

public abstract class ClassGenerator {
    protected final ProcessingEnvironment env;

    private final Filer filer;

    public ClassGenerator(ProcessingEnvironment env) {
        this.env = env;
        this.filer = env.getFiler();
    }

    public TypeSpec generate(String packageName, ClassName className, List<VariableElement> fields) throws IOException {
        TypeSpec.Builder viewClassBuilder = TypeSpec
                .classBuilder(className)
                .addSuperinterface(Serializable.class)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL);

        List<FieldSpec> fieldSpecs = fields.stream().map(this::mapField).filter(Objects::nonNull).collect(Collectors.toList());
        List<MethodSpec> methodSpecs = fieldSpecs.stream().map(this::mapMethod).filter(Objects::nonNull).collect(Collectors.toList());

        viewClassBuilder.addFields(fieldSpecs);
        viewClassBuilder.addMethods(methodSpecs);

        final MethodSpec constructor = Utils.createConstructor(fieldSpecs);
        final List<MethodSpec> getters = Utils.createGetters(fieldSpecs);

        viewClassBuilder.addMethod(constructor);
        viewClassBuilder.addMethods(getters);

        final TypeSpec viewClass = viewClassBuilder.build();
        JavaFile classF = JavaFile.builder(packageName, viewClass).build();
        classF.writeTo(filer);
        return viewClass;
    }

    protected abstract FieldSpec mapField(VariableElement field);
    protected abstract MethodSpec mapMethod(FieldSpec field);

}
