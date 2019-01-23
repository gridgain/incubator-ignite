package test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.bridge.PermittedOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.sockjs.BridgeOptions;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;

/**
 *
 */
public class TestWebServer extends AbstractVerticle {
    /**
     *
     */
    private static final SimpleDateFormat DEBUG_DATE_FMT = new SimpleDateFormat("HH:mm:ss,SSS");

    /**
     *
     */
    private Map<ServerWebSocket, String> agentSockets = new ConcurrentHashMap<>();

    /**
     *
     */
    private static void log(Object s) {
        System.out.println('[' + DEBUG_DATE_FMT.format(new Date(System.currentTimeMillis())) + "] [" +
            Thread.currentThread().getName() + ']' + ' ' + s);
    }

    /**
     *
     */
    public static void main(String... args) {
        log("Starting server...");

        Vertx.vertx().deployVerticle(new TestWebServer());

        log("Started client!");
    }

    /** {@inheritDoc} */
    @Override public void start() {
        SockJSHandler wsHandler = SockJSHandler.create(vertx);

        BridgeOptions allAccessOptions = new BridgeOptions()
            .addInboundPermitted(new PermittedOptions())
            .addOutboundPermitted(new PermittedOptions());

        wsHandler.bridge(allAccessOptions,
            be -> {
                log("BE: " + be.type() + " " + be.getRawMessage());
                be.complete(true);
            });
//            .socketHandler(s -> {
//                log("socketHandler: " + s.writeHandlerID());
//
//                s.handler(buf -> log("Received: " + buf.toString()));
//            });

//        vertx.eventBus().consumer("fromBrowser", msg -> log("From browser: " + msg.body()));

        Router router = Router.router(vertx);

        router.route("/eventbus/*").handler(wsHandler);

//        vertx.setPeriodic(3000L, t -> {
//            JsonObject json = new JsonObject();
//            json.put("data", UUID.randomUUID().toString());
//
//            vertx.eventBus().publish("toBrowser", json);
//        });

        vertx
            .createHttpServer()
            .requestHandler(router)
            // .websocketHandler(this::websocketHandler) // If this line present connection from Browser takes a lot of time!
            .listen(3000);
    }

    /**
     *
     */
    private void websocketHandler(ServerWebSocket ws) {
        log("webSocketHandler: " + ws.path());

        ws.handler(buf -> {
            JsonObject msg = buf.toJsonObject();

            log("Received via WebSocket: " + msg);

            String agent = msg.getString("agentId");

            if (agent != null)
                agentSockets.putIfAbsent(ws, agent);
        });
    }
}
