

package org.apache.ignite.configuration.internal.processor.pojo;

import com.squareup.javapoet.FieldSpec;
import javax.lang.model.element.VariableElement;

public class FieldMapping {

    private final VariableElement variableElement;

    private final FieldSpec fieldSpec;

    public FieldMapping(VariableElement variableElement, FieldSpec fieldSpec) {
        this.variableElement = variableElement;
        this.fieldSpec = fieldSpec;
    }

    public VariableElement getVariableElement() {
        return variableElement;
    }

    public FieldSpec getFieldSpec() {
        return fieldSpec;
    }
}
