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

package org.apache.ignite.examples.ml.inference.spark.modelparser;

import java.io.FileNotFoundException;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.examples.ml.tutorial.TitanicUtils;
import org.apache.ignite.ml.clustering.kmeans.KMeansModel;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.sparkmodelparser.SparkModelParser;
import org.apache.ignite.ml.sparkmodelparser.SupportedSparkModels;

/**
 * Run KMeans model loaded from snappy.parquet file.
 * The snappy.parquet file was generated by Spark MLLib model.write.overwrite().save(..) operator.
 * <p>
 * You can change the test data used in this example and re-run it to explore this algorithm further.</p>
 */
public class KMeansFromSparkExample {
    /** Path to Spark KMeans model. */
    public static final String SPARK_MDL_PATH = "examples/src/main/resources/models/spark/serialized/kmeans";

    /** Run example. */
    public static void main(String[] args) throws FileNotFoundException {
        System.out.println();
        System.out.println(">>> K-means model loaded from Spark through serialization over partitioned dataset usage example started.");
        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            IgniteCache<Integer, Vector> dataCache = null;
            try {
                dataCache = TitanicUtils.readPassengers(ignite);

                final Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<Integer>(0, 5, 6, 4).labeled(1);

                KMeansModel mdl = (KMeansModel)SparkModelParser.parse(
                    SPARK_MDL_PATH,
                    SupportedSparkModels.KMEANS
                );

                System.out.println(">>> K-Means model: " + mdl);
                System.out.println(">>> ------------------------------------");
                System.out.println(">>> | Predicted cluster\t| Is survived\t|");
                System.out.println(">>> ------------------------------------");

                try (QueryCursor<Cache.Entry<Integer, Vector>> observations = dataCache.query(new ScanQuery<>())) {
                    for (Cache.Entry<Integer,Vector> observation : observations) {
                        Vector inputs = featureExtractor.apply(observation.getKey(), observation.getValue());
                        double isSurvived = lbExtractor.apply(observation.getKey(), observation.getValue());
                        double clusterId = mdl.predict(inputs);

                        System.out.printf(">>> | %.4f\t\t| %.4f\t\t|\n", clusterId, isSurvived);
                    }
                }

                System.out.println(">>> ---------------------------------");
            } finally {
                dataCache.destroy();
            }
        }
    }
}
