package org.apache.ignite.console.agent;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;

/**
 *
 */
public class WebAgent {
    /**
     * @param args Args
     * @throws IOException if failed.
     */
    public static void main(String... args) throws IOException {
        System.out.println("Web Agent");

        WebAgent wa = new WebAgent();

        wa.start();

        //wa.sendMessage("{\"id\": \"zzzzz\", \"a\": 1, \"b\": 2}");

        System.in.read();
    }

    /**
     * zzzz
     */
    public void start() {
        try {
//            SslContextFactory sslContextFactory = new SslContextFactory();
//            Resource keyStoreResource = Resource.newResource(this.getClass().getResource("/truststore.jks"));
//            sslContextFactory.setKeyStoreResource(keyStoreResource);
//            sslContextFactory.setKeyStorePassword("password");
//            sslContextFactory.setKeyManagerPassword("password");
            WebSocketClient client = new WebSocketClient(/*sslContextFactory*/);
            String destUri = "ws://localhost:3000/eventbus";

            WebAgentSocket sock = new WebAgentSocket();
            try {
                client.start();

                URI echoUri = new URI(destUri);
                ClientUpgradeRequest req = new ClientUpgradeRequest();
                client.connect(sock, echoUri, req);
                System.out.printf("Connecting to : %s%n", echoUri);

                // wait for closed socket connection.
                sock.awaitClose(5, TimeUnit.SECONDS);
            }
            catch (Throwable t) {
                t.printStackTrace();
            }
            finally {
                try {
                    client.stop();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        catch (Throwable e) {
            e.printStackTrace();
        }
    }
}
