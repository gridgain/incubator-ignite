package org.apache.ignite.internal.configuration.initpojo;

import org.apache.ignite.internal.configuration.setpojo.NamedBuilder;

import static org.apache.ignite.internal.configuration.Keys.CONSISTENT_ID;
import static org.apache.ignite.internal.configuration.Keys.PORT;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class InitNode extends NamedBuilder {

    public InitNode(String name) {
        super(name);
    }

    public InitNode port(int port){
        changes.put(PORT, port);

        return this;
    }

    public InitNode consistentId(String consistentId){
        changes.put(CONSISTENT_ID, consistentId);

        return this;
    }

    public static InitNode initNode(String name){
        return new InitNode(name);
    }
}
