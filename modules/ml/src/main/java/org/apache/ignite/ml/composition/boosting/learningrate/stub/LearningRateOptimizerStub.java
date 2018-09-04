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

package org.apache.ignite.ml.composition.boosting.learningrate.stub;

import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.boosting.learningrate.LearningRateOptimizer;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.FeatureMatrixWithLabelsOnHeapData;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteTriFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/** Stub implementation of {@link LearningRateOptimizer} returning constant weight. */
public class LearningRateOptimizerStub<K,V> extends LearningRateOptimizer<K, V> {
    /** Serial version uid. */
    private static final long serialVersionUID = -6537333094072064826L;

    /** Constant Weight. */
    private final double weight;

    /**
     * Creates an instance of LearningRateOptimizerStub.
     *
     * @param sampleSize Sample size.
     * @param externalLbToInternalMapping External label to internal mapping.
     * @param lossGradient Loss gradient.
     * @param fExtractor F extractor.
     * @param lbExtractor Label extractor.
     * @param constantWeight Constant weight.
     */
    public LearningRateOptimizerStub(long sampleSize,
        IgniteFunction<Double, Double> externalLbToInternalMapping,
        IgniteTriFunction<Long, Double, Double, Double> lossGradient,
        IgniteBiFunction<K, V, Vector> fExtractor,
        IgniteBiFunction<K, V, Double> lbExtractor, double constantWeight) {

        super(sampleSize, externalLbToInternalMapping, lossGradient, fExtractor, lbExtractor);
        this.weight = constantWeight;
    }

    /** {@inheritDoc} */
    @Override public double learnRate(DatasetBuilder<K, V> builder, ModelsComposition currentComposition,
        Model<Vector, Double> newModel) {

        return weight;
    }

    /** {@inheritDoc} */
    @Override public double learnRate(Dataset<EmptyContext, ? extends FeatureMatrixWithLabelsOnHeapData> dataset,
        ModelsComposition currentComposition, Model<Vector, Double> newModel) {

        return weight;
    }
}
