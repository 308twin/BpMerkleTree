package btree4j.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TypeWithTime {
    long time;
    OperationType type;

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TypeWithTime) {
            TypeWithTime other = (TypeWithTime) obj;
            return time == other.time && type == other.type;
        }
        return false;
    }

    public enum OperationType {
        INSERT, DELETE, UPDATE
    }
}

