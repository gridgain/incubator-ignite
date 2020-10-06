package org.apache.ignite.internal.configuration.selector;

import java.util.List;
import org.apache.ignite.internal.configuration.Keys;
import org.apache.ignite.internal.configuration.getpojo.Node;
import org.apache.ignite.internal.configuration.initpojo.InitNode;
import org.apache.ignite.internal.configuration.internalconfig.NodeConfiguration;
import org.apache.ignite.internal.configuration.setpojo.ChangeNode;

import static org.apache.ignite.internal.configuration.Keys.LOCAL_BASELINE_NODES;
import static org.apache.ignite.internal.configuration.Keys.LOCAL_BASELINE_NODES_CONSISTENT_ID;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class NodeSelector extends Selector<Node, List<ChangeNode>, List<InitNode>, NodeConfiguration> {
    public static final SameSelector<Integer> PORT = new SameSelector<>(Keys.LOCAL_BASELINE_NODES_PORT);
    public static final SameSelector<Integer> LOCAL_BASELINE_NODES_PORT = new SameSelector<>(Keys.LOCAL_BASELINE_NODES_PORT);
    public static final SameSelector<String> CONSISTENT_ID = new SameSelector<>(LOCAL_BASELINE_NODES_CONSISTENT_ID);

    public NodeSelector() {
        super(LOCAL_BASELINE_NODES);
    }
}
