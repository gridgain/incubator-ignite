package org.apache.ignite.console.agent;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.security.KeyStore;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;

/**
 *
 */
public class WebAgent {
    /**
     * @param args Args
     * @throws IOException if failed.
     */
    public static void main(String... args) throws Exception {
        WebSocketAgentClient webAgentClient = new WebSocketAgentClient(new URI("wss://localhost:8887"));

        // load up the key store
        String STORETYPE = "JKS";
        String KEYSTORE = "keystore.jks";
        String STOREPASSWORD = "storepassword";
        String KEYPASSWORD = "keypassword";

        KeyStore ks = KeyStore.getInstance(STORETYPE);
        File kf = new File(KEYSTORE);
        ks.load(new FileInputStream(kf), STOREPASSWORD.toCharArray());

        KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
        kmf.init(ks, KEYPASSWORD.toCharArray());
        TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
        tmf.init(ks);

        SSLContext sslContext = null;
        sslContext = SSLContext.getInstance("TLS");
        sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        // sslContext.init( null, null, null ); // will use java's default key and trust store which is sufficient unless you deal with self-signed certificates

        SSLSocketFactory factory = sslContext.getSocketFactory();// (SSLSocketFactory) SSLSocketFactory.getDefault();

        webAgentClient.setSocketFactory(factory);

        webAgentClient.connectBlocking();

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            String line = reader.readLine();
            if (line.equals("close"))
                webAgentClient.closeBlocking();
            else if (line.equals("open"))
                webAgentClient.reconnect();
            else
                webAgentClient.send(line);
        }
    }

//    /**
//     * zzzz
//     */
//    public void start() {
//        try {
////            SslContextFactory sslContextFactory = new SslContextFactory();
////            Resource keyStoreResource = Resource.newResource(this.getClass().getResource("/truststore.jks"));
////            sslContextFactory.setKeyStoreResource(keyStoreResource);
////            sslContextFactory.setKeyStorePassword("password");
////            sslContextFactory.setKeyManagerPassword("password");
//            WebSocketClient client = new WebSocketClient(/*sslContextFactory*/);
//            String destUri = "ws://localhost:3000/eventbus";
//
//            WebSocketAgentClient sock = new WebSocketAgentClient();
//            try {
//                client.start();
//
//                URI echoUri = new URI(destUri);
//                ClientUpgradeRequest req = new ClientUpgradeRequest();
//                client.connect(sock, echoUri, req);
//                System.out.printf("Connecting to : %s%n", echoUri);
//
//                // wait for closed socket connection.
//                sock.awaitClose(5, TimeUnit.SECONDS);
//            }
//            catch (Throwable t) {
//                t.printStackTrace();
//            }
//            finally {
//                try {
//                    client.stop();
//                }
//                catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//        catch (Throwable e) {
//            e.printStackTrace();
//        }
//    }
}
