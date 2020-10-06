package org.apache.ignite.internal.configuration.getpojo;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class Node {
    private final String consistentId;
    private final int port;

    public Node(String id, int port) {
        consistentId = id;
        this.port = port;
    }

    @Override public String toString() {
        return "Node{" +
            "consistentId='" + consistentId + '\'' +
            ", port=" + port +
            '}';
    }
}
