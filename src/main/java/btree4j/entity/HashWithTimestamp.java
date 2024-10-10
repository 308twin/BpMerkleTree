package btree4j.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class HashWithTimestamp {
    private String hash;
    private long timestamp;
}
