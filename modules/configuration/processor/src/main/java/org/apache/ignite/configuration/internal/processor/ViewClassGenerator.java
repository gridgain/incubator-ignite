package org.apache.ignite.configuration.internal.processor;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import java.util.Collections;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeMirror;
import org.apache.ignite.configuration.internal.annotation.Config;
import org.apache.ignite.configuration.internal.annotation.NamedConfig;
import org.apache.ignite.configuration.internal.property.NamedList;

public class ViewClassGenerator extends ClassGenerator {

    public ViewClassGenerator(ProcessingEnvironment env) {
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
            fieldType = Utils.getViewName(confClass);
            if (namedConfigAnnotation != null) {
                fieldType = ParameterizedTypeName.get(ClassName.get(NamedList.class), fieldType);
            }
            name = name.replace("Configuration", "");
        }

        return FieldSpec.builder(fieldType, name, Modifier.PRIVATE, Modifier.FINAL).build();
    }

    @Override protected MethodSpec mapMethod(FieldSpec field) {
        return null;
    }
}
