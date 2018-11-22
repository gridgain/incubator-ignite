package org.apache.ignite.spi.discovery.tcp;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.discovery.CustomEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public class TcpDiscoveryListenerTest extends GridCommonAbstractTest {
    static final CopyOnWriteArrayList<TestCustomDiscoveryMessage> messages = new CopyOnWriteArrayList<>();

    static final CopyOnWriteArrayList<TestCustomDiscoveryAckMessage> acks = new CopyOnWriteArrayList<>();

    static final Listener1 listener1 = new Listener1();

    static final Listener2 listener2 = new Listener2();

    private static class Listener1 implements CustomEventListener<TestCustomDiscoveryMessage> {

        @Override
        public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd, TestCustomDiscoveryMessage msg) {
            messages.add(msg);
        }
    }

    private static class Listener2 implements CustomEventListener<TestCustomDiscoveryAckMessage> {

        @Override
        public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd, TestCustomDiscoveryAckMessage msg) {
            acks.add(msg);
        }
    }

    public void test1() throws Exception {
        List<IgniteEx> ignites = new ArrayList<>();

        ignites.add(startGrid(0));
        ignites.add(startGrid(1));
        ignites.add(startGrid(2));
        ignites.add(startGrid(3));
        ignites.add(startGrid(4));
        ignites.add(startGrid(5));

        ignites.forEach(new Consumer<IgniteEx>() {
            @Override
            public void accept(IgniteEx igniteEx) {
                igniteEx.context().discovery().setCustomEventListener(TestCustomDiscoveryMessage.class, listener1);
                igniteEx.context().discovery().setCustomEventListener(TestCustomDiscoveryAckMessage.class, listener2);
            }
        });

        stopGrid(3, true);
        ignites.get(0).context().discovery().sendCustomEvent(new TestCustomDiscoveryMessage(1));

        while(acks.size() != 5) {

        }

        assertEquals(5, messages.size());

        assertEquals(1, messages.get(0).corrId);
    }

}
