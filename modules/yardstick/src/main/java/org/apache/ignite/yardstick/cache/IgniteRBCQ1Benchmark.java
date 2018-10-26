package org.apache.ignite.yardstick.cache;

import java.util.Map;

public class IgniteRBCQ1Benchmark extends IgniteRBCAbstractBenchmark {

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> map) throws Exception {
        executeQuery(   "SELECT SOURCEBOOKNAME, " +
                        "S.Label1, " +
                        "SUM(AMOUNT) " +
                        "FROM  	Position P, " +
                        "Sensitivity S " +
                        "WHERE 	P.EodPositionId = S.EodPositionId " +
                        "AND 	S.RiskType = 'Risk_IRCurve' " +
                        "AND 	S.Label2 = 'OIL_BRENT' " +
                        "GROUP BY SOURCEBOOKNAME, Label1 ");

        return true;
    }
}
