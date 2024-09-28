package btree4j.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

import btree4j.server.WebSocketHandlerForHash;
import btree4j.server.WebSocketHandlerForRecord;

@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    // private final WebSocketHandler webSocketHandler;

    // public WebSocketConfig(WebSocketHandler webSocketHandler) {
    //     this.webSocketHandler = webSocketHandler;
    // }


    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {        // 注册 WebSocket 处理器
     
        registry.addHandler(new WebSocketHandlerForHash(), "/ws/hash")
                .setAllowedOrigins("*"); // 允许跨域
        registry.addHandler(new WebSocketHandlerForRecord(), "/ws/records")
                .setAllowedOrigins("*"); // 允许跨域
    }
}
