package org.apache.ignite.internal.configuration.setpojo;

import static org.apache.ignite.internal.configuration.Keys.PORT;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class ChangeNode extends Builder {

    public ChangeNode port(int port){
        changes.put(PORT, port);

        return this;
    }

    public static ChangeNode changeNode(){
        return new ChangeNode();
    }

    public static NList<ChangeNode> changeNodes() {
        return new NList<>();
    }
}
