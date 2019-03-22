package org.apache.ignite.console.agent;

import java.net.URI;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

/**
 * Socket to send data from web agent.
 */
public class WebSocketAgentClient extends WebSocketClient {
    /**
     * @param serverUri Server URI.
     */
    public WebSocketAgentClient(URI serverUri) {
        super(serverUri);
    }

    /** {@inheritDoc} */
    @Override public void onOpen(ServerHandshake handshakedata) {
        System.out.println("Connected");

    }

    /** {@inheritDoc} */
    @Override public void onMessage(String message) {
        System.out.println("got: " + message);

    }

    /** {@inheritDoc} */
    @Override public void onClose(int code, String reason, boolean remote) {
        System.out.println("Disconnected");

    }

    /** {@inheritDoc} */
    @Override public void onError(Exception ex) {
        ex.printStackTrace();

    }
}
