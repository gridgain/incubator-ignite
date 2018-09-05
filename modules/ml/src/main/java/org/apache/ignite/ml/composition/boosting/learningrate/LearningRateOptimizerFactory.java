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

import org.apache.ignite.ml.composition.boosting.loss.Loss;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Factory interface for LearningRateOptimizerFactory.
 */
public interface LearningRateOptimizerFactory {
    /**
     * Create an instance of LearningRateOptimizer.
     *
     * @param sampleSize Sample size.
     * @param externalLbToInternalMapping External label to internal mapping.
     * @param loss Loss function.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor Label extractor.
     * @return LearningRateOptimizer instance.
     */
    public abstract <K,V> LearningRateOptimizer<K,V> create(long sampleSize,
        IgniteFunction<Double, Double> externalLbToInternalMapping,
        IgniteFunction<Double, Double> internalLbToExternalMapping,
        Loss loss, IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, Double> lbExtractor);
}
