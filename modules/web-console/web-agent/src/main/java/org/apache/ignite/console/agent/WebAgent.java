package org.apache.ignite.console.agent;

import java.io.IOException;
import java.net.URI;
import javax.websocket.ClientEndpoint;
import javax.websocket.ContainerProvider;
import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.WebSocketContainer;

@ClientEndpoint
public class WebAgent {
    public static void main(String... args) throws IOException {
        System.out.println("Web Agent");

        WebAgent wa = new WebAgent();

        wa.start();

        wa.sendMessage("{\"id\": \"zzzzz\", \"a\": 1, \"b\": 2}");

        System.in.read();
    }

    private Session session;

    public void start() {
        try {
            WebSocketContainer container = ContainerProvider.getWebSocketContainer();

            container.connectToServer(this, new URI("ws://127.0.0.1:3000/eventbus"));

        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @OnOpen
    public void onOpen(Session session){
        System.out.println("OPENED");
        this.session=session;
    }

    @OnClose
    public void onClose(Session session){
        System.out.println("CLOSED");
    }

    @OnMessage
    public void onMessage(String message, Session session){
        System.out.println("MSG: " + message);
    }

    public void sendMessage(String message){
        try {
            session.getBasicRemote().sendText(message);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
