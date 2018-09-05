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

import java.io.Serializable;
import java.util.Comparator;
import java.util.Optional;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.boosting.learningrate.LearningRateOptimizer;
import org.apache.ignite.ml.composition.boosting.loss.Loss;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.feature.ObjectHistogram;
import org.apache.ignite.ml.dataset.primitive.FeatureMatrixWithLabelsOnHeapData;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;

/**
 * Implementation of learning rate optimizer using histogram to calculate learning rate optimum. This algorithm estimate
 * sum of absolute errors on dataset partitions, merge them and select learning rate corresponding to minimum of
 * aggregated sum.
 */
public class OnHistogramLearningOptimizer<K, V> extends LearningRateOptimizer<K, V> {
    /** Serial version uid. */
    private static final long serialVersionUID = -893322637324001504L;

    /** Sample size. */
    private final long sampleSize;

    /** External label to internal mapping. */
    private final IgniteFunction<Double, Double> externalLbToInternalMapping;

    /** Internal label to external mapping. */
    private final IgniteFunction<Double, Double> internalLbToExternalMapping;

    /** Loss function. */
    private final Loss loss;

    /** Precision. */
    private final double maxRateVal;

    /** Max rate value. */
    private final double bucketSize;

    /**
     * Creates an instance of OnHistogramLearningOptimizer.
     *
     * @param sampleSize Sample size.
     * @param externalLbToInternalMapping External label to internal mapping.
     * @param loss Loss function.
     * @param featureExtractor Feature extractor.
     * @param lblExtractor Lbl extractor.
     * @param internalLbToExternalMapping
     * @param precision Precision.
     * @param maxRateVal Max rate value.
     */
    public OnHistogramLearningOptimizer(long sampleSize, IgniteFunction<Double, Double> externalLbToInternalMapping,
        Loss loss, IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, Double> lblExtractor,
        IgniteFunction<Double, Double> internalLbToExternalMapping,
        double precision, double maxRateVal) {

        super(featureExtractor, lblExtractor);

        this.sampleSize = sampleSize;
        this.externalLbToInternalMapping = externalLbToInternalMapping;
        this.internalLbToExternalMapping = internalLbToExternalMapping;
        this.loss = loss;
        this.bucketSize = precision;
        this.maxRateVal = maxRateVal;
    }

    /** {@inheritDoc} */
    @Override public double learnRate(Dataset<EmptyContext, ? extends FeatureMatrixWithLabelsOnHeapData> dataset,
        ModelsComposition currComposition, Model<Vector, Double> newMdl) {

        ObjectHistogram<HistogramTuple> learningRateHist = dataset.compute(
            data -> computeErrorsOnPartitions(data, currComposition, newMdl),
            this::mergeHistograms
        );

        if (learningRateHist == null)
            return bucketSize;

        return learnRate(learningRateHist);
    }

    /**
     * @param data Data.
     * @param currComposition Current composition.
     * @param newMdl New model.
     */
    private ObjectHistogram<HistogramTuple> computeErrorsOnPartitions(FeatureMatrixWithLabelsOnHeapData data,
        ModelsComposition currComposition, Model<Vector, Double> newMdl) {

        ObjectHistogram<HistogramTuple> res = new ObjectHistogram<>(x -> (int)(x.rate / bucketSize), x -> x.error);
        for (double rateVal = bucketSize; rateVal <= maxRateVal; rateVal += bucketSize) {
            for (int i = 0; i < data.getLabels().length; i++) {
                Vector features = VectorUtils.of(data.getFeatures()[i]);
                Double lbl = externalLbToInternalMapping.apply(data.getLabels()[i]);
                Double currMdlAnswer = currComposition.apply(features);
                Double newMdlAnswer = newMdl.apply(features);

//                Double error = Math.pow(lbl - (currMdlAnswer + rateVal * newMdlAnswer), 2); //TODO: we need valid loss function
                Double externalLbl = internalLbToExternalMapping.apply(currMdlAnswer + rateVal * newMdlAnswer);
                Double error = !lbl.equals(externalLbl) ? 1.0 : 0.0; //TODO: we need valid loss function
                res.addElement(new HistogramTuple(rateVal, error));
            }
        }
        return res;
    }

    /**
     * @param left Left.
     * @param right Right.
     * @return merged histograms.
     */
    private ObjectHistogram<HistogramTuple> mergeHistograms(ObjectHistogram<HistogramTuple> left,
        ObjectHistogram<HistogramTuple> right) {

        if (left == null)
            return right;
        if (right == null)
            return left;

        return left.plus(right);
    }

    /**
     * Computes weight of new model for composition.
     *
     * @param hist Error hist.
     * @return learning rate.
     */
    private double learnRate(ObjectHistogram<HistogramTuple> hist) {
        Optional<HistogramTuple> min = hist.buckets().stream().map(buckId -> getHistTuple(hist, buckId))
            .filter(Optional::isPresent).map(Optional::get)
            .min(Comparator.comparingDouble(tuple -> tuple.error));

        return min.map(x -> x.rate).orElse(bucketSize);
    }

    /**
     * Restore HistogramTuple.
     *
     * @param hist History.
     * @param buckId Buck id.
     * @return HistogramTuple instance.
     */
    private Optional<HistogramTuple> getHistTuple(ObjectHistogram<HistogramTuple> hist, Integer buckId) {
        return hist.getValue(buckId).map(error -> new HistogramTuple(buckId * bucketSize, error));
    }

    /** */
    private static class HistogramTuple implements Serializable {
        /** Serial version uid. */
        private static final long serialVersionUID = 1244084359529696936L;

        /** Learning Rate. */
        private final double rate;

        /** Error sum. */
        private final double error;

        /**
         * Creates an instance of HistogramTuple.
         *
         * @param rate Learning Rate.
         * @param error Error sum.
         */
        public HistogramTuple(double rate, double error) {
            this.rate = rate;
            this.error = error;
        }
    }
}
