package dm.tcp;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;

import java.util.Collection;
import java.util.Random;

public class IgniteCommunicationNode {
    private static final int VALUE_SIZE = 10 * 1024 * 1024;
    private static final String CACHE_NAME = "cache";

    public static void main(String[] args) throws InterruptedException {
        new IgniteCommunicationNode().start();
    }

    private void start() throws InterruptedException {
        Ignite ignite = Ignition.start("config/ignite.xml");

        byte[] val = value(VALUE_SIZE);

        IgniteCache<Integer, byte[]> cache = ignite.getOrCreateCache(CACHE_NAME);

        int iteration = 0;

        while (true) {
            Affinity<Integer> affinity = ignite.affinity(CACHE_NAME);

            Collection<ClusterNode> nodes = ignite.cluster().forRemotes().nodes();

            if (!nodes.isEmpty()) {
                for (ClusterNode rmtNode : nodes) {
                    int key = primaryKeyForNode(affinity, rmtNode);

                    cache.put(key, val);
                    cache.get(key);
                }

                iteration++;

                if (iteration % 500 == 0) {
                    ignite.log().info(iteration + " iterations passed.");
                }
            } else {
                Thread.sleep(1000);
            }
        }
    }

    private int primaryKeyForNode(Affinity<Integer> affinity, ClusterNode node) {
        int key = 0;

        while (!affinity.isPrimary(node, key)) {
            key++;
        }

        return key;
    }

    private byte[] value(int len) {
        byte[] val = new byte[len];

        Random random = new Random(System.currentTimeMillis());
        random.nextBytes(val);

        return val;
    }
}
