package btree4j.entity;

import lombok.Data;

@Data
public class TableConsistEntity {
    private String dbName;
    private String tableName;
    private boolean isConsist;
    private String newestHash;
}
