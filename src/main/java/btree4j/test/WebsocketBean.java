package btree4j.test;

import javax.annotation.PostConstruct;
import org.springframework.stereotype.Component;
@Component
public class WebsocketBean {
    @PostConstruct
    public void init() {
        System.out.println("WebsocketBean的PostConstruct方法被调用");
    }
}
