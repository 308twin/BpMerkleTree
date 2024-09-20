package btree4j.entity;

import lombok.Data;

import java.util.LinkedHashMap;

@Data
public class MerkleHashEntity {
    private LinkedHashMap<String, Long> rootHashHistory;
    private String tableName;
    private String dbName;
}
