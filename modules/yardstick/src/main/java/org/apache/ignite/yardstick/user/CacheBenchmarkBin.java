package org.apache.ignite.yardstick.user;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;

import java.util.Map;
import java.util.Random;

public class CacheBenchmarkBin extends IgniteAbstractBenchmark {

    public static int MAX_TRADES = 10000;
    public static int MAX_TIDS = 50;
    Random r = new Random();
    Ignite i;
    IgniteCompute compute;

    @Override
    public boolean test(Map<Object, Object> map) throws Exception {
        Double quantity = r.nextDouble();
        Double price = r.nextDouble();
        Integer tid = r.nextInt(MAX_TIDS);
        IgniteAtomicLong atomicLong = i.atomicLong("tradeId",0,true);
        Long tradeId = atomicLong.incrementAndGet();
        compute.affinityCall("TradeCache",tradeId, new ActionCallable(tradeId,tid,quantity,price));
        return true;
    }

    @Override
    public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);
        i = ignite();
        compute = i.compute();
        IgniteCache<Integer,Trade> tradeCache = i.cache("TradeCache");
        IgniteCache<Integer,Position> positionCache = i.cache("PositionCache");

        for(int i=0; i<MAX_TRADES; i++) {
            int tid = r.nextInt(MAX_TIDS);
            Trade t = new Trade(Long.valueOf(i),tid,r.nextDouble(),r.nextDouble());
            Position position = positionCache.get(Integer.valueOf(tid));
            if(position == null) {
                position = new Position(tid,0.0);
            }
            tradeCache.put(i,t);
            positionCache.put(tid,position);
        }
    }
}
