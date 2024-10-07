package btree4j.server;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import org.springframework.web.socket.CloseStatus;
import javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;

//@Component
@ConditionalOnProperty(name = "my.custom.config.isServer", havingValue = "false")
public class WebSocketClientForHash {

    @Value("${socket.server.uriForHash}")
    private String serverUri;


    @PostConstruct
    public void startClient() {
        StandardWebSocketClient client = new StandardWebSocketClient();
        try {
            WebSocketSession session = client.doHandshake(new MyClientWebSocketHandler(), serverUri).get();
            System.out.println("Connected to server(hash): " + serverUri);
        } catch (Exception e) {
            System.out.println("Failed to connect(hash): " + e.getMessage());
        }
    }

    private class MyClientWebSocketHandler extends TextWebSocketHandler {
        @Override
        public void afterConnectionEstablished(WebSocketSession session) throws Exception {
            System.out.println("WebSocket Client connected(hash)");
            session.sendMessage(new TextMessage("Hello from Client(hash)"));
        }

        @Override
        protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
            System.out.println("Received from server(hash): " + message.getPayload());
        }

        @Override
        public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
            System.out.println("WebSocket Client disconnected(hash)");
        }
    }
}
