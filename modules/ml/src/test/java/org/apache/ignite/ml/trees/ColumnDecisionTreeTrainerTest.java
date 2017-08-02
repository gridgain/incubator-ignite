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

package org.apache.ignite.ml.trees;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.Tracer;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.impls.matrix.SparseDistributedMatrix;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.structures.LabeledVectorDouble;
import org.apache.ignite.ml.trees.models.DecisionTreeModel;
import org.apache.ignite.ml.trees.trainers.columnbased.ColumnDecisionTreeMatrixInput;
import org.apache.ignite.ml.trees.trainers.columnbased.ColumnDecisionTreeTrainer;
import org.apache.ignite.ml.trees.trainers.columnbased.contsplitcalcs.ContinuousSplitCalculators;
import org.apache.ignite.ml.trees.trainers.columnbased.contsplitcalcs.VarianceSplitCalculator;
import org.apache.ignite.ml.trees.trainers.columnbased.regcalcs.RegionCalculators;
import org.junit.Test;

/** Tests behaviour of ColumnDecisionTreeTrainer. */
public class ColumnDecisionTreeTrainerTest extends BaseDecisionTreeTest {
    @Test
    public void testCacheMixed() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
        int totalPts = 1 << 10;
        int featCnt = 2;

        HashMap<Integer, Integer> catsInfo = new HashMap<>();
        catsInfo.put(1, 3);

        Random rnd = new Random(12349L);

        SplitDataGenerator<DenseLocalOnHeapVector> gen = new SplitDataGenerator<>(
            featCnt, catsInfo, () -> new DenseLocalOnHeapVector(featCnt + 1), rnd).
            split(0, 1, new int[] {0, 2}).
            split(1, 0, -10.0);

        testByGen(totalPts, catsInfo, gen, RegionCalculators.MEAN, rnd);
    }

    @Test
    public void testCacheCont() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
        int totalPts = 1 << 10;
        int featCnt = 12;

        HashMap<Integer, Integer> catsInfo = new HashMap<>();

        Random rnd = new Random(12349L);

        SplitDataGenerator<DenseLocalOnHeapVector> gen = new SplitDataGenerator<>(
            featCnt, catsInfo, () -> new DenseLocalOnHeapVector(featCnt + 1), rnd).
            split(0, 0, -10.0).
            split(1, 0, 0.0).
            split(1, 1, 2.0).
            split(3, 7, 50.0);

        testByGen(totalPts, catsInfo, gen, RegionCalculators.MEAN, rnd);
    }

    @Test
    public void testCacheCat() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
        int totalPts = 1 << 10;
        int featCnt = 12;

        HashMap<Integer, Integer> catsInfo = new HashMap<>();
        catsInfo.put(5, 7);

        Random rnd = new Random(12349L);

        SplitDataGenerator<DenseLocalOnHeapVector> gen = new SplitDataGenerator<>(
            featCnt, catsInfo, () -> new DenseLocalOnHeapVector(featCnt + 1), rnd).
            split(0, 5, new int[] {0, 2, 5});

        testByGen(totalPts, catsInfo, gen, RegionCalculators.MEAN, rnd);
    }

    private void testByGen(int totalPts, HashMap<Integer, Integer> catsInfo,
        SplitDataGenerator<DenseLocalOnHeapVector> gen, IgniteFunction<DoubleStream, Double> regCalc, Random rnd) {

        List<IgniteBiTuple<Integer, DenseLocalOnHeapVector>> lst = gen.
            points(totalPts, (i, rn) -> i + 1).
            collect(Collectors.toList());

        int featCnt = gen.featCnt();

        Collections.shuffle(lst, rnd);

        SparseDistributedMatrix m = new SparseDistributedMatrix(totalPts, featCnt + 1, StorageConstants.COLUMN_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);

        Map<Integer, List<LabeledVectorDouble>> byRegion = new HashMap<>();

        int i = 0;
        for (IgniteBiTuple<Integer, DenseLocalOnHeapVector> bt : lst) {
            byRegion.putIfAbsent(bt.get1(), new LinkedList<>());
            byRegion.get(bt.get1()).add(asLabeledVector(bt.get2().getStorage().data()));
            m.setRow(i, bt.get2().getStorage().data());
            i++;
        }

        ColumnDecisionTreeTrainer<VarianceSplitCalculator.VarianceData> trainer =
            new ColumnDecisionTreeTrainer<>(3, ContinuousSplitCalculators.VARIANCE, RegionCalculators.VARIANCE, regCalc);

        DecisionTreeModel mdl = trainer.train(new ColumnDecisionTreeMatrixInput(m, catsInfo));

        byRegion.keySet().stream().forEach(k -> {
            LabeledVectorDouble sp = byRegion.get(k).get(0);
            Tracer.showAscii(sp.vector());
            System.out.println("Act: " + sp.label() + " " + " pred: " + mdl.predict(sp.vector()));
            assert mdl.predict(sp.vector()) == sp.doubleLabel();
        });

        trainer.destroy();
    }
}
