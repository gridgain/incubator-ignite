package org.apache.ignite.console.agent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.StatusCode;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;

/**
 * Socket to send data from web agent.
 */
@WebSocket(maxTextMessageSize = 64 * 1024)
public class WebAgentSocket {
    /** */
    private final CountDownLatch closeLatch;

    /** */
    @SuppressWarnings("unused") private Session ses;

    /**
     *
     */
    public WebAgentSocket() {
        closeLatch = new CountDownLatch(1);
    }

    /**
     * @param duration Duration.
     * @param unit Time units.
     * @return {@code true} if awaited.
     * @throws InterruptedException If failed.
     */
    public boolean awaitClose(int duration, TimeUnit unit) throws InterruptedException {
        return closeLatch.await(duration, unit);
    }

    /**
     * @param statusCode Status code.
     * @param reason Reason.
     */
    @OnWebSocketClose
    public void onClose(int statusCode, String reason) {
        System.out.printf("Connection closed: %d - %s%n", statusCode, reason);
        ses = null;
        closeLatch.countDown(); // trigger latch
    }

    /**
     *
     * @param ses Session.
     */
    @OnWebSocketConnect
    public void onConnect(Session ses) {
        System.out.printf("Got connect: %s%n", ses);

        this.ses = ses;

        try {
            Future<Void> fut = ses.getRemote().sendStringByFuture("Hello");
            fut.get(2, TimeUnit.SECONDS); // wait for send to complete.

            fut = ses.getRemote().sendStringByFuture("Thanks for the conversation.");
            fut.get(2, TimeUnit.SECONDS); // wait for send to complete.

            ses.close(StatusCode.NORMAL, "I'm done");
        }
        catch (Throwable t) {
            t.printStackTrace();
        }
    }

    /**
     * @param msg Message.
     */
    @OnWebSocketMessage
    public void onMessage(String msg) {
        System.out.printf("Got msg: %s%n", msg);
    }
}
