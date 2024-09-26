package btree4j.entity;

import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListMap;

public class LimitedSizeConcurrentSkipListMapDescending extends ConcurrentSkipListMap<Long, String> {
    private final int maxSize;

    // masSize default = 200000
    public LimitedSizeConcurrentSkipListMapDescending(int maxSize) {
        super(new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                // 反向排序，o2 - o1 确保long大的在前
                return o2.compareTo(o1);
            }
        });
        this.maxSize = maxSize;
    }

    @Override
    public String put(Long key, String value) {
        String result = super.put(key, value);
        // 检查是否超过最大长度
        if (size() > maxSize) {
            // 超过长度时移除long最小的元素（即最后一个元素）
            Long lastKey = lastKey();
            remove(lastKey);
        }
        return result;
    }    
}
