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

package org.apache.ignite.ml.composition.boosting.convergence.simple;

import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.boosting.convergence.ConvergenceCheckStrategy;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteTriFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.tree.data.DecisionTreeData;

/**
 * This strategy skip estimating error on dataset step.
 * According to this strategy, training will stop after reaching the maximum number of iterations.
 *
 * @param <K> Type of a key in upstream data.
 * @param <V> Type of a value in upstream data.
 */
public class SimpleCheckConvergenceStgy<K,V> extends ConvergenceCheckStrategy<K,V> {
    /** Serial version uid. */
    private static final long serialVersionUID = 8534776439755210864L;

    /**
     * Creates an intance of SimpleCheckConvergenceStgy.
     *
     * @param sampleSize Sample size.
     * @param externalLbToInternalMapping External label to internal mapping.
     * @param lossGradient Loss gradient.
     * @param datasetBuilder Dataset builder.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor Label extractor.
     */
    public SimpleCheckConvergenceStgy(long sampleSize,
        IgniteFunction<Double, Double> externalLbToInternalMapping,
        IgniteTriFunction<Long, Double, Double, Double> lossGradient,
        DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, Double> lbExtractor) {

        super(sampleSize, externalLbToInternalMapping, lossGradient, datasetBuilder,
            featureExtractor, lbExtractor, 0.0);
    }

    @Override public boolean isConverged(ModelsComposition currMdl) {
        return false;
    }

    @Override public boolean isConverged(Dataset<EmptyContext, ? extends DecisionTreeData> dataset,
        ModelsComposition currMdl) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public Double computeMeanErrorOnDataset(Dataset<EmptyContext, ? extends DecisionTreeData> dataset,
        ModelsComposition mdl) {

        throw new UnsupportedOperationException();
    }
}
