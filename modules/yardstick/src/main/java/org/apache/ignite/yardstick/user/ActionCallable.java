package org.apache.ignite.yardstick.user;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.transactions.Transaction;

public class ActionCallable implements IgniteCallable<Long> {
    @IgniteInstanceResource
    private transient Ignite ignite;
    private final Double quantity;
    private final Double price;
    private final Integer tid;
    private final Long tradeId;

    public ActionCallable(Long tradeId, Integer tid, Double quantity, Double price) {
        this.quantity = quantity;
        this.price = price;
        this.tid = tid;
        this.tradeId = tradeId;
    }

    @Override
    public Long call() throws Exception {
        IgniteCache<Long, Trade> tradeCache = ignite.cache("TradeCache");
        IgniteCache<Integer, Position> positionCache = ignite.cache("PositionCache");
        try(Transaction tx = ignite.transactions().txStart()){
            Trade t = new Trade(tradeId,tid,quantity,price);
            Position position = new Position(tid,quantity);
            Position p = positionCache.getAndPutIfAbsent(tid,position);
            if(p != null){
                position = new Position(tid,position.getQuantity() + p.getQuantity());
                positionCache.get(tid);
                positionCache.get(tid);
                positionCache.get(tid);
                positionCache.put(tid,position);
            }
            tradeCache.put(t.getTradeId(),t);
            tradeCache.put(t.getTradeId(),t);
            tradeCache.put(t.getTradeId(),t);
            tradeCache.put(t.getTradeId(),t);
            tx.commit();
        }
        return tradeId;
    }
}
