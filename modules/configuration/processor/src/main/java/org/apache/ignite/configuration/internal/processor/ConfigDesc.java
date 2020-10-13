package org.apache.ignite.configuration.internal.processor;

import java.util.ArrayList;
import java.util.List;
import com.squareup.javapoet.TypeName;

public class ConfigDesc extends ConfigField {

    public List<ConfigField> fields = new ArrayList<>();

    public ConfigDesc(TypeName type, String name, TypeName view, TypeName init, TypeName change) {
        super(type, name, view, init, change);
    }
}
