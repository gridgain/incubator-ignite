package org.apache.ignite.yardstick.cache;

import java.util.Map;

public class IgniteRBCQ3Benchmark extends IgniteRBCAbstractBenchmark {

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> map) throws Exception {
        executeQuery(   "SELECT AMOUNT " +
                        "FROM  Position P, " +
                        "        Sensitivity S " +
                        "WHERE P.EodPositionId = S.EodPositionId " +
                        "AND S.RiskType = 'Risk_IRCurve' " +
                        "AND S.Label2 = 'OIL_BRENT' " +
                        "AND SourceBookName = 'bookb259' " +
                        "AND QUALIFIER = 'CFI' " +
                        "LIMIT 1000");

        return true;
    }
}
