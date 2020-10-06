package org.apache.ignite.internal.configuration.internalconfig;

import java.util.Map;
import org.apache.ignite.internal.configuration.ConfigTreeVisitor;
import org.apache.ignite.internal.configuration.getpojo.Node;
import org.apache.ignite.internal.configuration.setpojo.Builder;

import static org.apache.ignite.internal.configuration.Keys.CONSISTENT_ID;
import static org.apache.ignite.internal.configuration.Keys.NODE;
import static org.apache.ignite.internal.configuration.Keys.PORT;
import static org.apache.ignite.internal.configuration.Keys.concat;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class NodeConfiguration extends DynamicConfiguration<Node> {
    private final DynamicProperty<String> consistentId = new DynamicProperty<>(CONSISTENT_ID, "none");
    private final DynamicProperty<Integer> port = new DynamicProperty<>(PORT, 667);

    public NodeConfiguration() {
        super(NODE);
    }

    @Override void updateValue(Map<String, Object> map) {
        if(map.containsKey(key())) {
            Map<String, Object> changes = ((Builder)map.get(key())).changes();
            consistentId.updateValue(changes);

            port.updateValue(changes);
        }
    }

    @Override public void accept(String path, ConfigTreeVisitor visitor) {
        visitor.visit(path, this);

        path = concat(path, key());

        consistentId.accept(path, visitor);
        port.accept(path, visitor);
    }

    @Override public Node toView() {
        return new Node(consistentId.toView(), port.toView());
    }
}
