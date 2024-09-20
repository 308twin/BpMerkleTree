package btree4j.cache;

import org.springframework.context.annotation.Configuration;
import btree4j.entity.MerkleHashEntity;
import org.springframework.context.annotation.Bean;

import java.util.*;

@Configuration
public class MerkleHashCache {

    @Bean("localHashs")
    public List<MerkleHashEntity> localHashs() {
        return new ArrayList<>();
    }

    @Bean("remoteHashs")
    public List<MerkleHashEntity> remoteHashs() {
        return new ArrayList<>();
    }

    @Bean("orderedTreemap")
    public Map<Long,String> orderedTreemap() {
        return new  TreeMap<>(new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                // 升序
                return o1.compareTo(o2);
            }
        });
    }

}
