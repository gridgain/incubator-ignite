package org.apache.ignite.yardstick.cache;

import java.util.Map;

public class IgniteRBCDataLoadingBenchmark extends IgniteRBCAbstractBenchmark {

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> map) throws Exception {
        loadData();

        return true;
    }
}
