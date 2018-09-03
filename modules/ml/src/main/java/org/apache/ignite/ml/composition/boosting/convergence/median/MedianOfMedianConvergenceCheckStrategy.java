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

package org.apache.ignite.ml.composition.boosting.convergence.median;

import java.util.Arrays;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.boosting.convergence.ConvergenceCheckStrategy;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteTriFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.tree.data.DecisionTreeData;
import org.jetbrains.annotations.NotNull;

/**
 * Use median of median on partitions value of errors for estimating error on dataset. This algorithm may be less
 * sensitive to
 *
 * @param <K> Type of a key in upstream data.
 * @param <V> Type of a value in upstream data.
 */
public class MedianOfMedianConvergenceCheckStrategy<K, V> extends ConvergenceCheckStrategy<K, V> {
    /** Serial version uid. */
    private static final long serialVersionUID = 4902502002933415287L;

    /**
     * Creates an instance of MedianOfMedianConvergenceCheckStrategy.
     *
     * @param sampleSize Sample size.
     * @param lblMapping External label to internal mapping.
     * @param lossGradient Loss gradient.
     * @param datasetBuilder Dataset builder.
     * @param fExtr Feature extractor.
     * @param lbExtr Label extractor.
     * @param precision Precision.
     */
    public MedianOfMedianConvergenceCheckStrategy(long sampleSize, IgniteFunction<Double, Double> lblMapping,
        IgniteTriFunction<Long, Double, Double, Double> lossGradient, DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> fExtr, IgniteBiFunction<K, V, Double> lbExtr, double precision) {

        super(sampleSize, lblMapping, lossGradient, datasetBuilder, fExtr, lbExtr, precision);
    }

    /** {@inheritDoc} */
    @Override public Double computeMeanErrorOnDataset(Dataset<EmptyContext, ? extends DecisionTreeData> dataset,
        ModelsComposition mdl) {

        double[] medians = dataset.compute(
            data -> computeMedian(mdl, data),
            this::reduce
        );

        if(medians == null)
            return Double.POSITIVE_INFINITY;
        return getMedian(medians);
    }

    @NotNull private double[] reduce(double[] left, double[] right) {
        if (left == null)
            return right;
        if(right == null)
            return left;

        double[] res = new double[left.length + right.length];
        for (int i = 0; i < left.length; i++)
            res[i] = left[i];
        for (int i = 0; i < right.length; i++)
            res[left.length + i] = right[i];
        return res;
    }

    @NotNull private double[] computeMedian(ModelsComposition mdl, DecisionTreeData data) {
        double[] errors = new double[data.getLabels().length];
        for (int i = 0; i < errors.length; i++)
            errors[i] = Math.abs(computeError(VectorUtils.of(data.getFeatures()[i]), data.getLabels()[i], mdl));
        return new double[] {getMedian(errors)};
    }

    private double getMedian(double[] errors) {
        if(errors.length == 0)
            return Double.POSITIVE_INFINITY;

        Arrays.sort(errors);
        final int middleIdx = (errors.length - 1) / 2;
        if (errors.length % 2 == 1)
            return errors[middleIdx];
        else
            return (errors[middleIdx + 1] + errors[middleIdx]) / 2;
    }
}
