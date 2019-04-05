package org.apache.ignite.ml.preprocessing;

import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.structures.LabeledVector;

public abstract class Preprocessor implements IgniteFunction<LabeledVector<Double>, LabeledVector<Double>> {

}
