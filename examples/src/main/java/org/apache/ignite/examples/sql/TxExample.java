package org.apache.ignite.examples.sql;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

public class TxExample {

    public static TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    private static final TransactionConcurrency TX_CONC = PESSIMISTIC;
    private static final TransactionIsolation TX_ISO = REPEATABLE_READ;

    @SuppressWarnings("unused")
    public static void main(String[] args) {
        try {
            Ignite primary = Ignition.start(config("pri", false));
            Ignite backup = Ignition.start(config("bak", false));

            Ignite cli = Ignition.start(config("cli", true));

            cli.createCache(
                new CacheConfiguration<Integer, Integer>()
                    .setName("cache")
                    .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                    .setCacheMode(CacheMode.PARTITIONED)
                    .setBackups(1)
            );

            Integer primaryKey = 0;
            Integer backupKey = 0;

            while (!cli.affinity("cache").isPrimary(((IgniteKernal)primary).localNode(), primaryKey))
                primaryKey++;

            while (!cli.affinity("cache").isBackup(((IgniteKernal)primary).localNode(), backupKey))
                backupKey++;

            System.out.println();
            System.out.println(">>> CLIENT ONE PHASE");

            try (Transaction tx = cli.transactions().txStart(TX_CONC, TX_ISO)) {
                cli.cache("cache").put(primaryKey, 1);

                tx.commit();
            }

            System.out.println();
            System.out.println(">>> CLIENT");

            try (Transaction tx = cli.transactions().txStart(TX_CONC, TX_ISO)) {
                cli.cache("cache").put(primaryKey, 1);
                cli.cache("cache").put(backupKey, 1);

                tx.commit();
            }

            System.out.println();
            System.out.println(">>> PRIMARY ONE PHASE");

            try (Transaction tx = primary.transactions().txStart(TX_CONC, TX_ISO)) {
                primary.cache("cache").put(primaryKey, 1);

                tx.commit();
            }

            System.out.println();
            System.out.println(">>> PRIMARY");

            try (Transaction tx = primary.transactions().txStart(TX_CONC, TX_ISO)) {
                primary.cache("cache").put(primaryKey, 1);
                primary.cache("cache").put(backupKey, 1);

                tx.commit();
            }

            System.out.println();
            System.out.println(">>> BACKUP ONE PHASE");

            try (Transaction tx = backup.transactions().txStart(TX_CONC, TX_ISO)) {
                backup.cache("cache").put(primaryKey, 1);

                tx.commit();
            }

            System.out.println();
        }
        finally {
            Ignition.stopAll(true);
        }
    }

    private static IgniteConfiguration config(final String name, boolean cli) {
        IgniteConfiguration cfg = new IgniteConfiguration().setIgniteInstanceName(name).setClientMode(cli);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi() {
            @Override public void sendMessage(ClusterNode node, Message msg) throws IgniteSpiException {
                traceMessage(node, msg);

                super.sendMessage(node, msg);
            }

            @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
                throws IgniteSpiException {
                traceMessage(node, msg);

                super.sendMessage(node, msg, ackC);
            }

            private void traceMessage(ClusterNode node, Message msg) {
                if (!(msg instanceof GridIoMessage))
                    return;

                Message msg0 = ((GridIoMessage)msg).message();

                GridKernalContext ctx =((IgniteKernal)this.ignite()).context();

                String locNodeName = ctx.igniteInstanceName();
                String rmtNodeName = node.attribute(ATTR_IGNITE_INSTANCE_NAME);

                System.out.println(">>> SEND " + locNodeName + " -> " + rmtNodeName + ": " + msg0 + ']');
            }
        };

        cfg.setCommunicationSpi(commSpi);

        return cfg;
    }
}
