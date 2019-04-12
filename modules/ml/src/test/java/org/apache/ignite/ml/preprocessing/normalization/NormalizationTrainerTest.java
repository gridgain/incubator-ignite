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

package org.apache.ignite.ml.preprocessing.normalization;

import org.apache.ignite.ml.common.TrainerTest;
import org.apache.ignite.ml.preprocessing.binarization.BinarizationTrainer;

/**
 * Tests for {@link BinarizationTrainer}.
 */
public class NormalizationTrainerTest extends TrainerTest {
    /** Tests {@code fit()} method. */
    /*@Test
    public void testFit() {
        Map<Integer, double[]> data = new HashMap<>();
        data.put(1, new double[] {2, 4, 1});
        data.put(2, new double[] {1, 8, 22});
        data.put(3, new double[] {4, 10, 100});
        data.put(4, new double[] {0, 22, 300});

        DatasetBuilder<Integer, double[]> datasetBuilder = new LocalDatasetBuilder<>(data, parts);

        NormalizationTrainer<Integer, double[]> normalizationTrainer = new NormalizationTrainer<Integer, double[]>()
            .withP(3);

        assertEquals(3., normalizationTrainer.p(), 0);

        NormalizationPreprocessor<Integer, double[]> preprocessor = normalizationTrainer.fit(
            TestUtils.testEnvBuilder(),
            datasetBuilder,
            (k, v) -> VectorUtils.of(v)
        );

        assertEquals(normalizationTrainer.p(), preprocessor.p(), 0);

        assertArrayEquals(new double[] {0.125, 0.99, 0.125}, preprocessor.apply(5, new double[]{1., 8., 1.}).asArray(), 1e-2);
    }*/
}
