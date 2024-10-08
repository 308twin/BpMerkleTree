package btree4j.server;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.lang.Nullable;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.context.annotation.ComponentScan;

//@Configuration
//@EnableWebSocket
public class WebSocketAppForRecord implements WebSocketConfigurer {

    @Value("${my.custom.config.isServer:false}") // 默认值为 false
    private boolean isServer;

    private final WebSocketHandlerForRecord webSocketHandler;
    private final WebSocketClientForRecord webSocketClient;

    public WebSocketAppForRecord(WebSocketHandlerForRecord webSocketHandler, @Nullable WebSocketClientForRecord webSocketClient) {
        this.webSocketHandler = webSocketHandler;
        this.webSocketClient = webSocketClient;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        if (isServer) {
            // 如果是服务器模式，注册 WebSocket 处理器
            registry.addHandler(webSocketHandler, "/ws/record").setAllowedOrigins("*");
        }
    }

    // 在应用启动后立即执行
    @PostConstruct
    public void init() {
        if (isServer) {
            System.out.println("Running as WebSocket Server for Record");
            // 服务器端的 WebSocket 处理由 `registerWebSocketHandlers` 自动注册
        } else {
            System.out.println("Running as WebSocket Client for Record");
            // 启动 WebSocket 客户端
            webSocketClient.startClient();
        }
    }

}
