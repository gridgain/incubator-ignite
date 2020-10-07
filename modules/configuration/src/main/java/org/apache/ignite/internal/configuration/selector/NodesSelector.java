package org.apache.ignite.internal.configuration.selector;

import org.apache.ignite.internal.configuration.Keys;
import org.apache.ignite.internal.configuration.getpojo.Node;
import org.apache.ignite.internal.configuration.initpojo.InitNode;
import org.apache.ignite.internal.configuration.internalconfig.NodeConfiguration;
import org.apache.ignite.internal.configuration.setpojo.ChangeNode;
import org.apache.ignite.internal.configuration.setpojo.NList;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class NodesSelector extends Selector<Node, NList<ChangeNode>, NList<InitNode>, NodeConfiguration> {

    public NodesSelector() {
        super(Keys.LOCAL_BASELINE_NODES);
    }

    public NodeSelector node(String name) {
        return new NodeSelector(name);
    }
}
