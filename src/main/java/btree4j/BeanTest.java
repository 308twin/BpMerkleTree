package btree4j;

import javax.annotation.PostConstruct;

import org.springframework.stereotype.Component;

@Component
public class BeanTest {
    @PostConstruct
    public void init() {
        System.out.println("MyBean的PostConstruct方法被调用");
    }
}
