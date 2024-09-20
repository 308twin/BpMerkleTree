package btree4j.entity;

import lombok.Data;

@Data
public class HashWithTimestamp {
    private String hash;
    private long timestamp;
}
