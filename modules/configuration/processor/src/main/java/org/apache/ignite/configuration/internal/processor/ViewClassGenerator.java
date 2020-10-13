package org.apache.ignite.configuration.internal.processor;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.processing.Filer;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeMirror;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import org.apache.ignite.configuration.internal.annotation.Config;
import org.apache.ignite.configuration.internal.annotation.NamedConfig;
import org.apache.ignite.configuration.internal.property.NamedList;

public class ViewClassGenerator {
    private final ProcessingEnvironment env;

    public ViewClassGenerator(ProcessingEnvironment env) {
        this.env = env;
    }

    public TypeSpec generate(String packageName, ClassName className, List<VariableElement> fields) throws IOException {
        TypeSpec.Builder viewClassBuilder = TypeSpec
                .classBuilder(className)
                .addSuperinterface(Serializable.class)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL);

        List<FieldSpec> fieldSpecs = new ArrayList<>();

        for (VariableElement field : fields) {
            final Config configAnnotation = field.getAnnotation(Config.class);
            final NamedConfig namedConfigAnnotation = field.getAnnotation(NamedConfig.class);

            final TypeMirror type = field.asType();
            String name = field.getSimpleName().toString();

            TypeName fieldType = TypeName.get(type);
            if (namedConfigAnnotation != null || configAnnotation != null) {
                ClassName confClass = (ClassName) fieldType;
                fieldType = ClassName.get(confClass.packageName(), confClass.simpleName().replace("ConfigurationSchema", ""));
                if (namedConfigAnnotation != null) {
                    fieldType = ParameterizedTypeName.get(ClassName.get(NamedList.class), fieldType);
                }
                name = name.replace("Configuration", "");
            }

            final FieldSpec generatedField = FieldSpec.builder(fieldType, name, Modifier.PRIVATE, Modifier.FINAL).build();
            viewClassBuilder.addField(generatedField);

            fieldSpecs.add(generatedField);
        }

        final MethodSpec constructor = Utils.createConstructor(fieldSpecs);
        final List<MethodSpec> getters = Utils.createGetters(fieldSpecs);

        viewClassBuilder.addMethod(constructor);
        viewClassBuilder.addMethods(getters);

        final Filer filer = env.getFiler();

        final TypeSpec viewClass = viewClassBuilder.build();
        JavaFile classF = JavaFile.builder(packageName, viewClass).build();
        classF.writeTo(filer);
        return viewClass;
    }

}
