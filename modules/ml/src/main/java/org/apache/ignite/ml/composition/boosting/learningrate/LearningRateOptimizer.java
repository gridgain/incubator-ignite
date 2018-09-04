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

package org.apache.ignite.ml.composition.boosting.learningrate;

import java.io.Serializable;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.FeatureMatrixWithLabelsOnHeapData;
import org.apache.ignite.ml.dataset.primitive.FeatureMatrixWithLabelsOnHeapDataBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteTriFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Contains logic of learning rate computing for Gradient Boosting algorithms.
 *
 * @param <K> Type of a key in upstream data.
 * @param <V> Type of a value in upstream data.
 */
public abstract class LearningRateOptimizer<K,V> implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = -5396003172766116232L;

    /** Sample size. */
    protected long sampleSize;

    /** External label to internal mapping. */
    protected IgniteFunction<Double, Double> externalLbToInternalMapping;

    /** Loss of gradient. */
    protected IgniteTriFunction<Long, Double, Double, Double> lossGradient;

    /** Feature extractor. */
    protected IgniteBiFunction<K, V, Vector> fExtractor;

    /** Label extractor. */
    protected IgniteBiFunction<K, V, Double> lbExtractor;

    /**
     * Creates an instance of LearningRateOptimizer.
     *
     * @param sampleSize Sample size.
     * @param externalLbToInternalMapping External label to internal mapping.
     * @param lossGradient Loss gradient.
     * @param fExtractor F extractor.
     * @param lbExtractor Label extractor.
     */
    public LearningRateOptimizer(long sampleSize, IgniteFunction<Double, Double> externalLbToInternalMapping,
        IgniteTriFunction<Long, Double, Double, Double> lossGradient, IgniteBiFunction<K, V, Vector> fExtractor,
        IgniteBiFunction<K, V, Double> lbExtractor) {

        this.sampleSize = sampleSize;
        this.externalLbToInternalMapping = externalLbToInternalMapping;
        this.lossGradient = lossGradient;
        this.fExtractor = fExtractor;
        this.lbExtractor = lbExtractor;
    }

    /**
     * Computes weight of new model for composition.
     *
     * @param builder Builder.
     * @param currentComposition Current composition.
     * @param newModel New model.
     * @return weight of new model of composition.
     */
    public double learnRate(DatasetBuilder<K,V> builder, ModelsComposition currentComposition,
        Model<Vector, Double> newModel) {

        try(Dataset<EmptyContext, ? extends FeatureMatrixWithLabelsOnHeapData> dataset = builder.build(
            new EmptyContextBuilder<>(),
            new FeatureMatrixWithLabelsOnHeapDataBuilder<>(fExtractor, lbExtractor))) {

            return learnRate(dataset, currentComposition, newModel);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Computes weight of new model for composition.
     *
     * @param dataset Dataset.
     * @param currentComposition Current composition.
     * @param newModel New model.
     * @return weight of new model of composition.
     */
    public abstract double learnRate(Dataset<EmptyContext, ? extends FeatureMatrixWithLabelsOnHeapData> dataset,
        ModelsComposition currentComposition, Model<Vector, Double> newModel);
}
