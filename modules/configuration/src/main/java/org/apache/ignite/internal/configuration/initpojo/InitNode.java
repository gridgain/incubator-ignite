package org.apache.ignite.internal.configuration.initpojo;

import org.apache.ignite.internal.configuration.setpojo.Builder;
import org.apache.ignite.internal.configuration.setpojo.NList;

import static org.apache.ignite.internal.configuration.Keys.CONSISTENT_ID;
import static org.apache.ignite.internal.configuration.Keys.PORT;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class InitNode extends Builder {

    public InitNode port(int port){
        changes.put(PORT, port);

        return this;
    }

    public InitNode consistentId(String consistentId){
        changes.put(CONSISTENT_ID, consistentId);

        return this;
    }

    public static InitNode initNode(){
        return new InitNode();
    }

    public static NList<InitNode> initNodes() {
        return new NList<>();
    }
}
