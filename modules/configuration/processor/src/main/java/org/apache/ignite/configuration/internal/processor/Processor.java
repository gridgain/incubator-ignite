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
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
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
import java.util.stream.IntStream;
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
import org.apache.ignite.configuration.internal.processor.pojo.ChangeClassGenerator;
import org.apache.ignite.configuration.internal.processor.pojo.InitClassGenerator;
import org.apache.ignite.configuration.internal.processor.pojo.ViewClassGenerator;
import org.apache.ignite.configuration.internal.processor.validation.ValidationGenerator;
import org.apache.ignite.configuration.internal.property.DynamicProperty;
import org.apache.ignite.configuration.internal.selector.Selector;
import org.apache.ignite.configuration.internal.selector.BaseSelectors;

import static javax.lang.model.element.Modifier.FINAL;
import static javax.lang.model.element.Modifier.PUBLIC;
import static javax.lang.model.element.Modifier.STATIC;

/**
 * Annotation processor that produces configuration classes.
 */
public class Processor extends AbstractProcessor {
    /** Generator of VIEW classes. */
    private ViewClassGenerator viewClassGenerator;

    /** Generator of INIT classes. */
    private InitClassGenerator initClassGenerator;

    /** Generator of CHANGE classes. */
    private ChangeClassGenerator changeClassGenerator;

    /** Class file writer. */
    private Filer filer;

    @Override public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
        filer = processingEnv.getFiler();
        final ValidationGenerator validationGenerator = new ValidationGenerator();
        viewClassGenerator = new ViewClassGenerator(processingEnv, validationGenerator);
        initClassGenerator = new InitClassGenerator(processingEnv, validationGenerator);
        changeClassGenerator = new ChangeClassGenerator(processingEnv, validationGenerator);
    }

    @Override public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnvironment) {
        final Elements elementUtils = processingEnv.getElementUtils();

        Map<TypeName, ConfigDesc> props = new HashMap<>();

        List<ConfigDesc> roots = new ArrayList<>();

        String packageForUtil = "";

        final Set<? extends Element> annotatedConfigs = roundEnvironment.getElementsAnnotatedWith(Config.class);

        if (annotatedConfigs.isEmpty())
            return false;

        for (Element element : annotatedConfigs) {
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

            if (isRoot) {
                roots.add(configDesc);
                packageForUtil = packageName;
            }

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

                    viewClassType = Utils.getViewName((ClassName) baseType);
                    initClassType = Utils.getInitName((ClassName) baseType);
                    changeClassType = Utils.getChangeName((ClassName) baseType);

                    getMethodType = ParameterizedTypeName.get(ClassName.get(NamedListConfiguration.class), viewClassType, fieldType, initClassType, changeClassType);

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

                    final CodeBlock block = new ValidationGenerator().generateValidators(field);

                    initMethodBuilder.addStatement("add($L = new $T(qualifiedName, $S, $L))", fieldName, getMethodType, fieldName, block);
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
                TypeName dynConfViewClassType = ParameterizedTypeName.get(dynConfClass, viewClassTypeName, initClassName, changeClassName);
                configurationClassBuilder.superclass(dynConfViewClassType);
                final MethodSpec toViewMethod = createToViewMethod(viewClassTypeName, fields);
                configurationClassBuilder.addMethod(toViewMethod);
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                MethodSpec validateChangeMethod = changeClassGenerator.generate(packageName, changeClassName, fields);
                final MethodSpec changeMethod = createChangeMethod(changeClassName, fields);
                configurationClassBuilder.addMethod(changeMethod);
                configurationClassBuilder.addMethod(validateChangeMethod);
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                MethodSpec validateInitMethod = initClassGenerator.generate(packageName, initClassName, fields);
                final MethodSpec initMethod = createInitMethod(initClassName, fields);
                configurationClassBuilder.addMethod(initMethod);
                configurationClassBuilder.addMethod(validateInitMethod);
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

        JavaFile keysClassFile = JavaFile.builder(packageForUtil, keysClass.build()).build();
        try {
            keysClassFile.writeTo(filer);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write class: " + e.getMessage(), e);
        }

        ClassName selectorsClassName = ClassName.get(packageForUtil, "Selectors");

        final TypeSpec.Builder selectorsClass = TypeSpec.classBuilder(selectorsClassName).superclass(BaseSelectors.class).addModifiers(PUBLIC, FINAL);

        final CodeBlock.Builder selectorsStaticBlockBuilder = CodeBlock.builder();
        selectorsStaticBlockBuilder.addStatement("$T publicLookup = $T.publicLookup()", MethodHandles.Lookup.class, MethodHandles.class);

        selectorsStaticBlockBuilder
            .beginControlFlow("try");

        flattenConfig.forEach(s -> {
            final String varName = s.name.toUpperCase().replace(".", "_");

            TypeName t = s.type;
            if (Utils.isNamedConfiguration(t))
                t = Utils.unwrapNamedListConfigurationClass(t);

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

            TypeName selectorRec = Utils.getParameterized(ClassName.get(Selector.class), root.type, t, s.view, s.init, s.change);

            if (namedCount > 0) {
                final String methodName = varName + "_FN";

                final MethodSpec.Builder builder = MethodSpec.methodBuilder(methodName);

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

                final String collect = IntStream.range(0, namedCount).mapToObj(i -> "$T.class").collect(Collectors.joining(","));
                List<Object> params = new ArrayList<>();
                params.add(MethodHandle.class);
                params.add(methodName);
                params.add(selectorsClassName);
                params.add(methodName);
                params.add(MethodType.class);
                params.add(Selector.class);
                for (int i = 0; i < namedCount; i++) {
                    params.add(String.class);
                }

                selectorsStaticBlockBuilder.addStatement("$T $L = publicLookup.findStatic($T.class, $S, $T.methodType($T.class, " + collect + "))", params.toArray());

                selectorsStaticBlockBuilder.addStatement("put($S, $L)", s.name, methodName);
            } else {
                final String fieldName = varName + "_REC";
                selectorsClass.addField(
                    FieldSpec.builder(selectorRec, fieldName)
                        .addModifiers(PUBLIC, STATIC, FINAL)
                        .initializer("(root) -> root$L", methodCall)
                        .build()
                );
                selectorsStaticBlockBuilder.addStatement("put($S, $L)", s.name, fieldName);
            }
        });

        selectorsStaticBlockBuilder
            .nextControlFlow("catch ($T e)", Exception.class)
            .endControlFlow();

        selectorsClass.addStaticBlock(selectorsStaticBlockBuilder.build());

        JavaFile selectorsClassFile = JavaFile.builder(selectorsClassName.packageName(), selectorsClass.build()).build();
        try {
            selectorsClassFile.writeTo(filer);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write class: " + e.getMessage(), e);
        }

        return true;
    }

    private Set<ConfigChain> traverseConfigTree(ConfigDesc root, Map<TypeName, ConfigDesc> props) {
        Set<ConfigChain> res = new HashSet<>();
        Deque<ConfigChain> propsStack = new LinkedList<>();

        ConfigChain rootChain = new ConfigChain(root.type, root.name, root.name, root.view, root.init, root.change, null);

        propsStack.addFirst(rootChain);
        while (!propsStack.isEmpty()) {
            final ConfigChain current = propsStack.pollFirst();

            TypeName type = current.type;

            if (Utils.isNamedConfiguration(type))
                type = Utils.unwrapNamedListConfigurationClass(current.type);

            final ConfigDesc configDesc = props.get(type);
            final List<ConfigField> propertiesList = configDesc.fields;

            if (current.name != null && !current.name.isEmpty()) {
                res.add(current);
            }

            for (ConfigField property : propertiesList) {
                String regex = "([a-z])([A-Z]+)";
                String replacement = "$1_$2";

                String qualifiedName = property.name
                    .replaceAll(regex, replacement)
                    .toLowerCase();

                if (current.name != null && !current.name.isEmpty())
                    qualifiedName = current.name + "." + qualifiedName;

                final ConfigChain newChainElement = new ConfigChain(property.type, qualifiedName, property.name, property.view, property.init, property.change, current);

                boolean isNamedConfig = false;
                if (property.type instanceof ParameterizedTypeName) {
                    final ParameterizedTypeName parameterized = (ParameterizedTypeName) property.type;
                    if (parameterized.rawType.equals(ClassName.get(NamedListConfiguration.class))) {
                        isNamedConfig = true;
                    }
                }

                if (props.containsKey(property.type) || isNamedConfig)
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

    /**
     * Create init method (accepts INIT object) for configuration class.
     *
     * @param type INIT method type.
     * @param variables List of INIT object's fields.
     * @return Init method.
     */
    public MethodSpec createInitMethod(TypeName type, List<VariableElement> variables) {
        final CodeBlock.Builder builder = CodeBlock.builder();

        builder.addStatement("validate(initial)");

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

    /**
     * Create change method (accepts CHANGE object) for configuration class.
     *
     * @param type CHANGE method type.
     * @param variables List of CHANGE object's fields.
     * @return Change method.
     */
    public MethodSpec createChangeMethod(TypeName type, List<VariableElement> variables) {
        final CodeBlock.Builder builder = CodeBlock.builder();

        builder.addStatement("validate(changes)");

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

    /** {@inheritDoc} */
    @Override public Set<String> getSupportedAnnotationTypes() {
        return Collections.singleton(Config.class.getCanonicalName());
    }

    /** {@inheritDoc} */
    @Override public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.RELEASE_8;
    }
}
