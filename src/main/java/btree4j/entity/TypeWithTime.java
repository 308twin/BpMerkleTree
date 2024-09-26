package btree4j.entity;

import lombok.Data;

@Data
public class TypeWithTime {
    long time;
    int type;

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TypeWithTime) {
            TypeWithTime other = (TypeWithTime) obj;
            return time == other.time && type == other.type;
        }
        return false;
    }
}
