package org.apache.ignite.internal;

import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.mxbean.TransactionsMXBean;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class TransactionsMXBeanImplTest extends GridCommonAbstractTest {
    /**
     *
     */
    public void testTransactions() throws Exception {
        IgniteEx ignite = startGrid(0);
        TransactionsMXBean txMXBean = mxBean();

        ignite.transactions().txStart();
        assertEquals(1, txMXBean.getLocalActiveTransactions().size());

        txMXBean.stopTransaction(txMXBean.getLocalActiveTransactions().keySet().iterator().next());
        assertEquals(0, txMXBean.getLocalActiveTransactions().size());
    }

    /**
     *
     */
    private TransactionsMXBean mxBean() throws Exception {
        ObjectName mbeanName = U.makeMBeanName(getTestIgniteInstanceName(0), "Transactions", TransactionsMXBeanImpl.class.getSimpleName());
        MBeanServer mbeanSrv = ManagementFactory.getPlatformMBeanServer();
        if (!mbeanSrv.isRegistered(mbeanName))
            fail("MBean is not registered: " + mbeanName.getCanonicalName());
        return MBeanServerInvocationHandler.newProxyInstance(mbeanSrv, mbeanName, TransactionsMXBean.class, true);
    }
}
