package org.apache.ignite.configuration.internal.processor;

import com.squareup.javapoet.TypeName;

public class ConfigField {

    public final TypeName type;

    public final String name;

    public final TypeName view;
    public final TypeName init;
    public final TypeName change;

    public ConfigField(TypeName type, String name, TypeName view, TypeName init, TypeName change) {
        this.type = type;
        this.name = name;
        this.view = view;
        this.init = init;
        this.change = change;
    }
}
