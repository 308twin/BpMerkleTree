package btree4j.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Service;
import btree4j.entity.MerkleHashEntity;
import java.util.*;

@Service
@ConfigurationProperties(prefix = "my.custom.config")
public class CompareService {
    private boolean isServer;
    private List<MerkleHashEntity> localHashs;
    private List<MerkleHashEntity> remoteHashs;

    @Autowired
    public CompareService(@Qualifier("localHashs") List<MerkleHashEntity> localHashs, List<MerkleHashEntity> remoteHashs) {
        this.localHashs = localHashs;
        this.remoteHashs = remoteHashs;
    }
}
