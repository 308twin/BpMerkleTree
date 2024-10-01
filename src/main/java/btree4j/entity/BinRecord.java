package btree4j.entity;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@ToString
public class BinRecord extends TypeWithTime implements Serializable{
    private static final long serialVersionUID = 1905122041950251207L;
    private String key;

    public BinRecord(String key, long time, OperationType type) {
        super(time, type);
        this.key = key;
    }


    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BinRecord) {
            BinRecord other = (BinRecord) obj;
            return super.equals(obj) && key.equals(other.key) ;
        }
        return false;
    }
    
}
