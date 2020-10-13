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

}
