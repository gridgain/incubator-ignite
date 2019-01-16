package my;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteOffHeapIterator;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class InMemoryGetTest extends GridCommonAbstractTest {

    @Override public void setUp() throws Exception {
        super.setUp();
        startGrids(1);
    }

    @Test
    public void readByOffHeapIterator() {
        Ignite ignite = grid(0);

        IgniteCache<Object, Object> cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        cache.put(1,1);

        IgniteOffHeapIterator it = cache.getByteIterator(1);

        if(it.remaining() >= 4)
            System.out.println(it.nextInt());

    }
}
