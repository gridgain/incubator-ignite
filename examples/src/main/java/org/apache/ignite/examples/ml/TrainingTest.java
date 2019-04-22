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

package org.apache.ignite.examples.ml;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.clustering.gmm.GmmTrainer;
import org.apache.ignite.ml.clustering.kmeans.KMeansTrainer;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.feature.FeatureMeta;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.knn.classification.KNNClassificationTrainer;
import org.apache.ignite.ml.knn.regression.KNNRegressionTrainer;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.naivebayes.gaussian.GaussianNaiveBayesTrainer;
import org.apache.ignite.ml.nn.Activators;
import org.apache.ignite.ml.nn.MLPTrainer;
import org.apache.ignite.ml.nn.MultilayerPerceptron;
import org.apache.ignite.ml.nn.UpdatesStrategy;
import org.apache.ignite.ml.nn.architecture.MLPArchitecture;
import org.apache.ignite.ml.optimization.LossFunctions;
import org.apache.ignite.ml.optimization.updatecalculators.RPropParameterUpdate;
import org.apache.ignite.ml.optimization.updatecalculators.RPropUpdateCalculator;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDParameterUpdate;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDUpdateCalculator;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.regressions.linear.LinearRegressionLSQRTrainer;
import org.apache.ignite.ml.regressions.linear.LinearRegressionSGDTrainer;
import org.apache.ignite.ml.regressions.logistic.LogisticRegressionSGDTrainer;
import org.apache.ignite.ml.selection.scoring.evaluator.Evaluator;
import org.apache.ignite.ml.selection.scoring.metric.Metric;
import org.apache.ignite.ml.selection.scoring.metric.classification.Accuracy;
import org.apache.ignite.ml.selection.scoring.metric.regression.RegressionMetricValues;
import org.apache.ignite.ml.selection.scoring.metric.regression.RegressionMetrics;
import org.apache.ignite.ml.svm.SVMLinearClassificationTrainer;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.tree.DecisionTreeClassificationTrainer;
import org.apache.ignite.ml.tree.DecisionTreeRegressionTrainer;
import org.apache.ignite.ml.tree.randomforest.RandomForestClassifierTrainer;
import org.apache.ignite.ml.tree.randomforest.RandomForestRegressionTrainer;
import org.apache.ignite.ml.util.MLSandboxDatasets;
import org.apache.ignite.ml.util.SandboxMLCache;
import org.jetbrains.annotations.NotNull;

/** */
public class TrainingTest {
    /** */
    private static List<DatasetTrainer<? extends IgniteModel, Double>> classifiers = Arrays.asList(
        new DecisionTreeClassificationTrainer(10, 1e-5),
        new RandomForestClassifierTrainer(featureMeta(4)),
//      new GDBBinaryClassifierOnTreesTrainer(0.01, 600, 10, 1e-5), // These models are unstable after IGNITE-11642.
//      new MLPTrainerAdapter(),                                    // We know it and we will investigate this problem.
        new GaussianNaiveBayesTrainer().withEquiprobableClasses(),
        new KNNClassificationTrainer(),
        new LogisticRegressionSGDTrainer(),
        new SVMLinearClassificationTrainer()
//        new ANNClassificationTrainer()                            // We don't have good accuracy in this case
//            .withK(2).withDistance(new EuclideanDistance())       // it seems to be a bug in algorithm.
    );

    /** */
    private static List<DatasetTrainer<? extends IgniteModel, Double>> regressors = Arrays.asList(
        new DecisionTreeRegressionTrainer(10, 1e-5),
        new RandomForestRegressionTrainer(featureMeta(13)),
//        new GDBRegressionOnTreesTrainer(0.01, 600, 10, 1e-5),
        new LinearRegressionLSQRTrainer(),
        new LinearRegressionSGDTrainer<>(new UpdatesStrategy<>(
            new RPropUpdateCalculator(),
            RPropParameterUpdate.SUM_LOCAL,
            RPropParameterUpdate.AVG
        ), 100000, 10, 100, 123L),
        new KNNRegressionTrainer()
    );

    /** */
    private static List<DatasetTrainer<? extends IgniteModel, Double>> clusterers = Arrays.asList(
        new KMeansTrainer().withAmountOfClusters(2).withDistance(new EuclideanDistance()),
        new GmmTrainer(2, 100)
    );

    /** */
    public static void main(String[] args) throws FileNotFoundException {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            IgniteCache<Integer, Vector> classificationCache = null;
            IgniteCache<Integer, Vector> regressionCache = null;
            try {
                classificationCache = prepareClassificationCache(ignite);

                Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<Integer>()
                    .labeled(Vectorizer.LabelCoordinate.FIRST);

                System.out.println(">>> Test classifiers.");
                testModels(ignite, classificationCache, vectorizer, classifiers,
                    Optional.of(new Accuracy<>()), m -> m > 0.9, infTime -> infTime < 1.);

                System.out.println(">>> Test regressors.");
                regressionCache = prepareRegressionCache(ignite);
                testModels(ignite, regressionCache, vectorizer, regressors,
                    Optional.of(new RegressionMetrics().withMetric(RegressionMetricValues::mae)),
                    m -> m < 5., infTime -> infTime < 1.);


                System.out.println(">>> Test clusterers.");
                testModels(ignite, classificationCache, vectorizer, clusterers,
                    Optional.empty(), m -> true, infTime -> infTime < 1.);
            } finally {
                if (classificationCache != null)
                    classificationCache.destroy();

                if (regressionCache != null)
                    regressionCache.destroy();
            }
        }
    }

    /** */
    private static void testModels(Ignite ignite, IgniteCache<Integer, Vector> cache,
        Vectorizer<Integer, Vector, Integer, Double> vectorizer,
        List<DatasetTrainer<? extends IgniteModel, Double>> trainers,
        Optional<Metric<Double>> metric,
        Predicate<Double> metricCheck, Predicate<Double> inferenceTimeCheck
    ) {
        for (DatasetTrainer<? extends IgniteModel<Vector, Double>, Double> trainer : trainers) {
            IgniteModel<Vector, Double> model = trainer.fit(ignite, cache, vectorizer);

            Double metricValue = metric.map(m -> Evaluator.evaluate(cache, model, vectorizer, m)).orElse(Double.NaN);

            int datasetSize = 0;
            long startTime = System.currentTimeMillis();

            for (int repeat = 0; repeat < 10; repeat++) {
                Iterator<Cache.Entry<Integer, Vector>> it = cache.query(new ScanQuery<Integer, Vector>()).iterator();
                while (it.hasNext()) {
                    Cache.Entry<Integer, Vector> next = it.next();
                    model.predict(vectorizer.extract(next.getKey(), next.getValue()).features());
                    datasetSize++;
                }
            }

            long endTime = System.currentTimeMillis();
            double infTime = ((double)endTime - startTime) / datasetSize;

            System.out.println(String.format(">>> Model [name = %s; metric = %.2f; inference ts = %.2f ms",
                model.getClass().getSimpleName(),
                metricValue, infTime
            ));


            A.ensure(metricCheck.test(metricValue), String.format("metric check is failed [value = %.2f]", metricValue));
            A.ensure(inferenceTimeCheck.test(infTime), String.format("inference check is failed [value = %.2f]", infTime));
        }
    }

    /** */
    private static IgniteCache<Integer, Vector> prepareClassificationCache(Ignite ignite) throws FileNotFoundException {
        return new SandboxMLCache(ignite).fillCacheWith(MLSandboxDatasets.TWO_CLASSED_IRIS);
    }

    /** */
    private static IgniteCache<Integer, Vector> prepareRegressionCache(Ignite ignite) throws FileNotFoundException {
        return new SandboxMLCache(ignite).fillCacheWith(MLSandboxDatasets.BOSTON_HOUSE_PRICES);
    }

    /** */
    private static List<FeatureMeta> featureMeta(int featuresCount) {
        return IntStream.range(0, featuresCount)
            .mapToObj(i -> new FeatureMeta("f" + i, i, false))
            .collect(Collectors.toList());
    }

    /** */
    private static class MLPTrainerAdapter extends DatasetTrainer<IgniteModel<Vector, Double>, Double> {
        MLPArchitecture arch = new MLPArchitecture(2).
            withAddedLayer(10, true, Activators.RELU).
            withAddedLayer(1, false, Activators.SIGMOID);

        // Define a neural network trainer.
        MLPTrainer<SimpleGDParameterUpdate> trainer = new MLPTrainer<>(
            arch,
            LossFunctions.MSE,
            new UpdatesStrategy<>(
                new SimpleGDUpdateCalculator(0.1),
                SimpleGDParameterUpdate.SUM_LOCAL,
                SimpleGDParameterUpdate.AVG
            ),
            3000,
            4,
            50,
            123L
        );

        /** {@inheritDoc} */
        @Override public <K, V> IgniteModel<Vector, Double> fit(DatasetBuilder<K, V> datasetBuilder,
            Preprocessor<K, V> preprocessor) {

            return learn(datasetBuilder, preprocessor);
        }

        /** {@inheritDoc} */
        @Override public boolean isUpdateable(IgniteModel<Vector, Double> mdl) {
            return true;
        }

        /** {@inheritDoc} */
        @Override protected <K, V> IgniteModel<Vector, Double> updateModel(IgniteModel<Vector, Double> mdl,
            DatasetBuilder<K, V> datasetBuilder, Preprocessor<K, V> preprocessor) {

            return learn(datasetBuilder, preprocessor);
        }

        /**
         * Some description.
         *
         * @param datasetBuilder Dataset builder.
         * @param preprocessor Preprocessor.
         */
        @NotNull private <K, V> IgniteModel<Vector, Double> learn(DatasetBuilder<K, V> datasetBuilder,
            Preprocessor<K, V> preprocessor) {
            MultilayerPerceptron perceptron = trainer.fit(datasetBuilder, preprocessor);
            return new IgniteModel<Vector, Double>() {
                @Override public Double predict(Vector input) {
                    return perceptron.predict(input.toMatrix(false)).get(0, 0);
                }
            };
        }
    }
}
