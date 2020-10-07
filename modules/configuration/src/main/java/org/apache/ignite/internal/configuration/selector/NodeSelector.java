package org.apache.ignite.internal.configuration.selector;

import java.util.List;
import org.apache.ignite.internal.configuration.Keys;
import org.apache.ignite.internal.configuration.getpojo.Node;
import org.apache.ignite.internal.configuration.initpojo.InitNode;
import org.apache.ignite.internal.configuration.internalconfig.DynamicProperty;
import org.apache.ignite.internal.configuration.internalconfig.NodeConfiguration;
import org.apache.ignite.internal.configuration.setpojo.ChangeNode;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class NodeSelector extends Selector<Node, List<ChangeNode>, List<InitNode>, NodeConfiguration> {

    public NodeSelector(String nodeName) {
        super(Keys.concat(Keys.LOCAL_BASELINE_NODES, nodeName));
    }

    public Selector<Integer, Integer, Integer, DynamicProperty<Integer>> port() {
        return new Selector<>(Keys.concat(key(), Keys.PORT));
    }

    public Selector<String, String, String, DynamicProperty<String>> consistentId() {
        return new Selector<>(Keys.concat(key(), Keys.CONSISTENT_ID));
    }
}
