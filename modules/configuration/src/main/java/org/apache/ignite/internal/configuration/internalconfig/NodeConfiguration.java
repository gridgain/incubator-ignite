package org.apache.ignite.internal.configuration.internalconfig;

import org.apache.ignite.internal.configuration.getpojo.Node;

import static org.apache.ignite.internal.configuration.Keys.CONSISTENT_ID;
import static org.apache.ignite.internal.configuration.Keys.PORT;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class NodeConfiguration extends DynamicConfiguration<Node> {
    private final DynamicProperty<String> consistentId = add(new DynamicProperty<>(CONSISTENT_ID, "none"));
    private final DynamicProperty<Integer> port = add(new DynamicProperty<>(PORT, 667));

    public NodeConfiguration(String name) {
        super(name);
    }

//    @Override public void accept(String path, ConfigTreeVisitor visitor) {
//        visitor.visit(path, this);
//
//        path = concat(path, key());
//
//        consistentId.accept(path, visitor);
//        port.accept(path, visitor);
//    }

    @Override public Node toView() {
        return new Node(consistentId.toView(), port.toView());
    }
}
