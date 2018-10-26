package org.apache.ignite.yardstick.cache;

import java.util.Map;

public class IgniteRBCQ2Benchmark extends IgniteRBCAbstractBenchmark {

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> map) throws Exception {
        executeQuery(   "SELECT s.QUALIFIER, " +
                        "S.Label1, " +
                        "SUM(AMOUNT) " +
                        "FROM  Position P, " +
                        "        Sensitivity S " +
                        "WHERE P.EodPositionId = S.EodPositionId " +
                        "AND S.RiskType = 'Risk_IRCurve' " +
                        "AND S.Label2 = 'OIL_BRENT' " +
                        "AND     SourceBookName = 'booka1' " +
                        "GROUP BY Qualifier, Label1 ");

        return true;
    }
}
