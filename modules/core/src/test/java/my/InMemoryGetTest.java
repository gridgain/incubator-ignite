package my;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteOffHeapIterator;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

public class InMemoryGetTest extends GridCommonAbstractTest {

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrids(1);
    }

    @Override public void setUp() throws Exception {
        super.setUp();
    }

    public void testReadByOffHeapIterator() {
        Ignite ignite = grid(0);

        IgniteCache<Object, Object> cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        cache.put(1,1);

        IgniteOffHeapIterator it = cache.getByteIterator(1);
        System.out.println(it.nextByte());

//        if(it.remaining() >= 4)
            System.out.println(it.nextInt());

    }
}
