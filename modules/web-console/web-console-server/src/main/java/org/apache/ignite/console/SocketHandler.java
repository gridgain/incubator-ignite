package org.apache.ignite.console;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

/**
 * Todo
 */
@Component
public class SocketHandler extends TextWebSocketHandler {
    /** */
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** */
    List<WebSocketSession> sessions = new CopyOnWriteArrayList<>();

    /** {@inheritDoc} */
    @Override public void handleTextMessage(WebSocketSession ses, TextMessage msg) {
            try {
                for (WebSocketSession ws : sessions) {
                    if (ws.isOpen()) {
                        String payload = msg.getPayload();

                        Operation op = MAPPER.readValue(payload, Operation.class);

                        System.out.println("WS Request:  [ses: " + ws.getId() + ", data: " + payload + "]");

                        Result res = new Result();
                        res.setId(op.getId());
                        res.setResult(op.getA() + op.getB());

                        ws.sendMessage(new TextMessage(MAPPER.writeValueAsString(res)));
                    }
                    else {
                        System.out.println("Removed closed session: " + ws.getId());

                        sessions.remove(ws);
                    }
                }
            }
            catch (Throwable e) {
                System.out.println("Error: " + e.getMessage());
            }
    }

    /** {@inheritDoc} */
    @Override public void afterConnectionEstablished(WebSocketSession ses) {
        System.out.println("New session: " + ses.getId());

        //Messages will be sent to all users.
        sessions.add(ses);
    }
}
