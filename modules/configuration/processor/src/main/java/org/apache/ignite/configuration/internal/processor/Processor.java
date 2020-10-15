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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.util.Elements;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import org.apache.ignite.configuration.internal.DynamicConfiguration;
import org.apache.ignite.configuration.internal.NamedListConfiguration;
import org.apache.ignite.configuration.internal.annotation.Config;
import org.apache.ignite.configuration.internal.annotation.NamedConfig;
import org.apache.ignite.configuration.internal.annotation.Value;
import org.apache.ignite.configuration.internal.property.DynamicProperty;
import org.apache.ignite.configuration.internal.selector.AnotherSelector;
import org.apache.ignite.configuration.internal.selector.Selector;

import static javax.lang.model.element.Modifier.FINAL;
import static javax.lang.model.element.Modifier.PUBLIC;
import static javax.lang.model.element.Modifier.STATIC;

/**
 *
 */
public class Processor extends AbstractProcessor {
    /** */
    private ProcessingEnvironment processingEnv;

    /** */
    private ViewClassGenerator viewClassGenerator;

    /** */
    private ChangeClassGenerator changeClassGenerator;

    /** */
    private InitClassGenerator initClassGenerator;

    /** */
    private Filer filer;

    @Override public synchronized void init(ProcessingEnvironment processingEnv) {
        this.processingEnv = processingEnv;
        super.init(processingEnv);
        this.filer = processingEnv.getFiler();
        viewClassGenerator = new ViewClassGenerator(processingEnv);
        changeClassGenerator = new ChangeClassGenerator(processingEnv);
        initClassGenerator = new InitClassGenerator(processingEnv);
    }

    @Override public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnvironment) {
        final Elements elementUtils = processingEnv.getElementUtils();

        Map<TypeName, ConfigDesc> props = new HashMap<>();

        List<ConfigDesc> roots = new ArrayList<>();

        for (Element element : roundEnvironment.getElementsAnnotatedWith(Config.class)) {
            if (element.getKind() != ElementKind.CLASS) {
                continue;
            }
            TypeElement clazz = (TypeElement) element;

            final PackageElement elementPackage = elementUtils.getPackageOf(clazz);
            final String packageName = elementPackage.getQualifiedName().toString();

            final List<VariableElement> fields
                = clazz.getEnclosedElements().stream()
                .filter(el -> el.getKind() == ElementKind.FIELD)
                .map(el -> (VariableElement) el)
                .collect(Collectors.toList());

            final Config clazzConfigAnnotation = clazz.getAnnotation(Config.class);

            final String configName = clazzConfigAnnotation.value();
            final boolean isRoot = clazzConfigAnnotation.root();
            final ClassName schemaClassName = ClassName.get(packageName, clazz.getSimpleName().toString());
            final ClassName configClass = Utils.getConfigurationName(schemaClassName);

            ConfigDesc configDesc = new ConfigDesc(configClass, configName, Utils.getViewName(schemaClassName), Utils.getInitName(schemaClassName), Utils.getChangeName(schemaClassName));

            if (isRoot)
                roots.add(configDesc);

            TypeSpec.Builder configurationClassBuilder = TypeSpec
                    .classBuilder(configClass)
                    .addModifiers(PUBLIC, FINAL);

            final MethodSpec emptyConstructor = MethodSpec.constructorBuilder()
                    .addModifiers(PUBLIC)
                    .addStatement("this($S, $S)", "", configName)
                    .build();

            configurationClassBuilder.addMethod(emptyConstructor);

            CodeBlock.Builder initMethodBuilder = CodeBlock.builder();

            for (VariableElement field : fields) {
                TypeName getMethodType = null;

                final TypeName baseType = TypeName.get(field.asType());
                final String fieldName = field.getSimpleName().toString();

                TypeName unwrappedType = baseType;
                TypeName viewClassType = baseType;
                TypeName initClassType = baseType;
                TypeName changeClassType = baseType;

                final Config confAnnotation = field.getAnnotation(Config.class);
                if (confAnnotation != null) {
                    getMethodType = Utils.getConfigurationName((ClassName) baseType);

                    final FieldSpec nestedConfigField =
                        FieldSpec
                            .builder(getMethodType, fieldName, Modifier.PRIVATE, FINAL)
                            .build();

                    configurationClassBuilder.addField(nestedConfigField);

                    initMethodBuilder.addStatement("add($L = new $T(qualifiedName, $S))", fieldName, getMethodType, fieldName);

                    unwrappedType = getMethodType;
                    viewClassType = Utils.getViewName((ClassName) baseType);
                    initClassType = Utils.getInitName((ClassName) baseType);
                    changeClassType = Utils.getChangeName((ClassName) baseType);
                }

                final NamedConfig namedConfigAnnotation = field.getAnnotation(NamedConfig.class);
                if (namedConfigAnnotation != null) {
                    ClassName fieldType = Utils.getConfigurationName((ClassName) baseType);

                    ClassName viewType = Utils.getViewName((ClassName) baseType);

                    getMethodType = ParameterizedTypeName.get(ClassName.get(NamedListConfiguration.class), viewType, fieldType);

                    final FieldSpec nestedConfigField =
                            FieldSpec
                                .builder(getMethodType, fieldName, Modifier.PRIVATE, FINAL)
                                .build();

                    configurationClassBuilder.addField(nestedConfigField);

                    initMethodBuilder.addStatement("add($L = new $T(qualifiedName, $S, $T::new))", fieldName, getMethodType, fieldName, fieldType);
                }

                final Value valueAnnotation = field.getAnnotation(Value.class);
                if (valueAnnotation != null) {
                    ClassName dynPropClass = ClassName.get(DynamicProperty.class);

                    TypeName genericType = baseType;

                    if (genericType.isPrimitive()) {
                        genericType = genericType.box();
                    }

                    getMethodType = ParameterizedTypeName.get(dynPropClass, genericType);

                    final FieldSpec generatedField = FieldSpec.builder(getMethodType, fieldName, Modifier.PRIVATE, FINAL).build();

                    configurationClassBuilder.addField(generatedField);

                    initMethodBuilder.addStatement("add($L = new $T(qualifiedName, $S))", fieldName, getMethodType, fieldName);
                }

                configDesc.fields.add(new ConfigField(getMethodType, fieldName, viewClassType, initClassType, changeClassType));

                MethodSpec getMethod = MethodSpec
                        .methodBuilder(fieldName)
                        .addModifiers(PUBLIC, FINAL)
                        .returns(getMethodType)
                        .addStatement("return $L", fieldName)
                        .build();
                configurationClassBuilder.addMethod(getMethod);

                if (valueAnnotation != null) {
                    MethodSpec setMethod = MethodSpec
                            .methodBuilder(fieldName)
                            .addModifiers(PUBLIC, FINAL)
                            .addParameter(unwrappedType, fieldName)
                            .addStatement("this.$L.change($L)", fieldName, fieldName)
                            .build();
                    configurationClassBuilder.addMethod(setMethod);
                }
            }

            props.put(configClass, configDesc);

            final ClassName viewClassTypeName = Utils.getViewName(schemaClassName);
            final ClassName initClassName = Utils.getInitName(schemaClassName);
            final ClassName changeClassName = Utils.getChangeName(schemaClassName);
            try {
                viewClassGenerator.generate(packageName, viewClassTypeName, fields);
                ClassName dynConfClass = ClassName.get(DynamicConfiguration.class);
                TypeName dynConfViewClassType = ParameterizedTypeName.get(dynConfClass, viewClassTypeName, changeClassName, initClassName);
                configurationClassBuilder.superclass(dynConfViewClassType);
                final MethodSpec toViewMethod = createToViewMethod(viewClassTypeName, fields);
                configurationClassBuilder.addMethod(toViewMethod);
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                changeClassGenerator.generate(packageName, changeClassName, fields);
                final MethodSpec changeMethod = createChangeMethod(changeClassName, fields);
                configurationClassBuilder.addMethod(changeMethod);
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                initClassGenerator.generate(packageName, initClassName, fields);
                final MethodSpec initMethod = createInitMethod(initClassName, fields);
                configurationClassBuilder.addMethod(initMethod);
            } catch (IOException e) {
                e.printStackTrace();
            }

            final MethodSpec constructorWithName = MethodSpec.constructorBuilder()
                    .addModifiers(PUBLIC)
                    .addParameter(String.class, "prefix")
                    .addParameter(String.class, "name")
                    .addStatement("super(prefix, name)")
                    .addCode(initMethodBuilder.build())
                    .build();
            configurationClassBuilder.addMethod(constructorWithName);

            JavaFile classF = JavaFile.builder(packageName, configurationClassBuilder.build()).build();
            try {
                classF.writeTo(filer);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        final TypeSpec.Builder keysClass = TypeSpec.classBuilder("Keys").addModifiers(PUBLIC, FINAL);

        final List<ConfigChain> flattenConfig = roots.stream().map((ConfigDesc cfg) -> traverseConfigTree(cfg, props)).flatMap(Set::stream).collect(Collectors.toList());

        flattenConfig.forEach(s -> {
            final String varName = s.name.toUpperCase().replace(".", "_");
            keysClass.addField(
                FieldSpec.builder(String.class, varName)
                    .addModifiers(PUBLIC, STATIC, FINAL)
                    .initializer("$S", s.name)
                    .build()
            );
        });

        JavaFile keysClassFile = JavaFile.builder("org.apache.ignite", keysClass.build()).build();
        try {
            keysClassFile.writeTo(filer);
        } catch (IOException e) {
            e.printStackTrace();
        }

        final TypeSpec.Builder selectorsClass = TypeSpec.classBuilder("Selectors").addModifiers(PUBLIC, FINAL);

        flattenConfig.forEach(s -> {
            final String varName = s.name.toUpperCase().replace(".", "_");

            TypeName t = s.type;
            if (Utils.isNamedConfiguration(t))
                t = Utils.unwrapConfigurationClass(t);

            TypeName selector = Utils.getParameterized(ClassName.get(Selector.class), s.view, s.change, s.init, t);

            selectorsClass.addField(
                FieldSpec.builder(selector, varName)
                    .addModifiers(PUBLIC, STATIC, FINAL)
                    .initializer("new $T(Keys.$L)", selector, varName)
                    .build()
            );

            String methodCall = "";

            ConfigChain current = s;
            ConfigChain root = null;
            int namedCount = 0;
            while (current != null) {
                boolean isNamed = false;
                if (Utils.isNamedConfiguration(current.type)) {
                    namedCount++;
                    isNamed = true;
                }

                if (current.parent == null)
                    root = current;
                else
                    methodCall = "." + current.originalName + "()" + (isNamed ? (".get(name" + (namedCount - 1) + ")") : "") + methodCall;

                current = current.parent;
            }

            TypeName selectorRec = Utils.getParameterized(ClassName.get(AnotherSelector.class), root.type, t);

            if (namedCount > 0) {
                final MethodSpec.Builder builder = MethodSpec.methodBuilder(varName + "_FN");

                for (int i = 0; i < namedCount; i++) {
                    builder.addParameter(String.class, "name" + i);
                }

                selectorsClass.addMethod(
                    builder
                        .returns(selectorRec)
                        .addModifiers(PUBLIC, STATIC, FINAL)
                        .addStatement("return (root) -> root$L", methodCall)
                        .build()
                );
            } else {
                selectorsClass.addField(
                    FieldSpec.builder(selectorRec, varName + "_REC")
                        .addModifiers(PUBLIC, STATIC, FINAL)
                        .initializer("(root) -> root$L", methodCall)
                        .build()
                );
            }
        });

        JavaFile selectorsClassFile = JavaFile.builder("org.apache.ignite", selectorsClass.build()).build();
        try {
            selectorsClassFile.writeTo(filer);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return true;
    }

    private Set<ConfigChain> traverseConfigTree(ConfigDesc root, Map<TypeName, ConfigDesc> props) {
        Set<ConfigChain> res = new HashSet<>();
        Deque<ConfigChain> propsStack = new LinkedList<>();

        ConfigChain rootChain = new ConfigChain(root.type, root.name, root.name, root.view, root.init, root.change, null);

        propsStack.addFirst(rootChain);
        while (!propsStack.isEmpty()) {
            final ConfigChain a = propsStack.pollFirst();

            TypeName type = Utils.unwrapConfigurationClass(a.type);

            final ConfigDesc configDesc = props.get(type);
            final List<ConfigField> propertiesList = configDesc.fields;

            if (a.name != null && !a.name.isEmpty()) {
                res.add(a);
            }

            for (ConfigField property : propertiesList) {
                String regex = "([a-z])([A-Z]+)";
                String replacement = "$1_$2";

                String qualifiedName = property.name
                        .replaceAll(regex, replacement)
                        .toLowerCase();

                if (a.name != null && !a.name.isEmpty())
                    qualifiedName = a.name + "." + qualifiedName;

                final ConfigChain newChainElement = new ConfigChain(property.type, qualifiedName, property.name, property.view, property.init, property.change, a);

                boolean z = false;
                if (property.type instanceof ParameterizedTypeName) {
                    final ParameterizedTypeName zzz = (ParameterizedTypeName) property.type;
                    if (zzz.rawType.equals(ClassName.get(NamedListConfiguration.class))) {
                        z = true;
                    }
                }

                if (props.containsKey(property.type) || z)
                    propsStack.add(newChainElement);
                else
                    res.add(newChainElement);

            }
        }
        return res;
    }

    public MethodSpec createToViewMethod(TypeName type, List<VariableElement> variables) {
        String args = variables.stream()
            .map(v -> v.getSimpleName().toString() + ".toView()")
            .collect(Collectors.joining(", "));

        final CodeBlock returnBlock = CodeBlock.builder()
                .add("return new $T($L)", type, args)
                .build();

        return MethodSpec.methodBuilder("toView")
            .addModifiers(PUBLIC)
            .addAnnotation(Override.class)
            .returns(type)
            .addStatement(returnBlock)
            .build();
    }

    public MethodSpec createChangeMethod(TypeName type, List<VariableElement> variables) {
        final CodeBlock.Builder builder = CodeBlock.builder();
        variables.forEach(variable -> {
            final Value valueAnnotation = variable.getAnnotation(Value.class);
            if (valueAnnotation != null && valueAnnotation.initOnly())
                return;

            final String name = variable.getSimpleName().toString();
            builder.beginControlFlow("if (changes.$L() != null)", name);
            builder.addStatement("$L.change(changes.$L())", name, name);
            builder.endControlFlow();
        });

        return MethodSpec.methodBuilder("change")
                .addModifiers(PUBLIC)
                .addAnnotation(Override.class)
                .addParameter(type, "changes")
                .addCode(builder.build())
                .build();
    }

    public MethodSpec createInitMethod(TypeName type, List<VariableElement> variables) {
        final CodeBlock.Builder builder = CodeBlock.builder();
        variables.forEach(variable -> {
            final String name = variable.getSimpleName().toString();
            builder.beginControlFlow("if (initial.$L() != null)", name);
            builder.addStatement("$L.init(initial.$L())", name, name);
            builder.endControlFlow();
        });

        return MethodSpec.methodBuilder("init")
                .addModifiers(PUBLIC)
                .addAnnotation(Override.class)
                .addParameter(type, "initial")
                .addCode(builder.build())
                .build();
    }

    @Override public Set<String> getSupportedAnnotationTypes() {
        return Collections.singleton(Config.class.getCanonicalName());
    }

    @Override public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.RELEASE_8;
    }
}
