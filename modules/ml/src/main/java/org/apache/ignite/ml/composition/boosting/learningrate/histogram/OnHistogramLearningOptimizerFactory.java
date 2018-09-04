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

package org.apache.ignite.ml.composition.boosting.learningrate.histogram;

import org.apache.ignite.ml.composition.boosting.learningrate.LearningRateOptimizer;
import org.apache.ignite.ml.composition.boosting.learningrate.LearningRateOptimizerFactory;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteTriFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/** Factory for OnHistogramLearningOptimizer. */
public class OnHistogramLearningOptimizerFactory implements LearningRateOptimizerFactory {
    /** Maximum of rate value. */
    private final double maxRateVal;

    /** Precision of histogram. */
    private final double precision;

    /**
     * Create an instance of OnHistogramLearningOptimizerFactory.
     *
     * @param maxRateVal Maximum of rate value.
     * @param precision Precision.
     */
    public OnHistogramLearningOptimizerFactory(double maxRateVal, double precision) {
        this.maxRateVal = maxRateVal;
        this.precision = precision;
    }

    /** {@inheritDoc} */
    @Override public <K, V> LearningRateOptimizer<K, V> create(long sampleSize,
        IgniteFunction<Double, Double> externalLbToInternalMapping,
        IgniteFunction<Double, Double> internalLbToExternalMapping,
        IgniteTriFunction<Long, Double, Double, Double> lossGradient, IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, Double> lbExtractor) {

        return new OnHistogramLearningOptimizer<>(sampleSize, externalLbToInternalMapping, lossGradient,
            featureExtractor, lbExtractor, internalLbToExternalMapping, precision, maxRateVal);
    }
}
