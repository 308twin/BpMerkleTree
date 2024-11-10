package btree4j.entity;

import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class LimitedSizeConcurrentSkipListMapDescending extends ConcurrentSkipListMap<Long, String> {
    private final int maxSize;
    private final ConcurrentHashMap<String, Long> valueMap = new ConcurrentHashMap<>();

    // maxSize default = 200000
    public LimitedSizeConcurrentSkipListMapDescending(int maxSize) {
        super(new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                // 反向排序，o2 - o1 确保 long 大的在前
                return o2.compareTo(o1);
            }
        });
        this.maxSize = maxSize;
    }

    @Override
    public String put(Long key, String value) {
        String previousValue = super.put(key, value);
        if (previousValue != null) {
            // 更新 valueMap 中的值
            valueMap.remove(previousValue);
        }
        valueMap.put(value, key);

        // 检查是否超过最大长度
        if (size() > maxSize) {
            // 超过长度时移除 long 最小的元素（即最后一个元素）
            Long lastKey = lastKey();
            String lastValue = remove(lastKey);
            valueMap.remove(lastValue);
        }
        return previousValue;
    }

    @Override
    public String remove(Object key) {
        String removedValue = super.remove(key);
        if (removedValue != null) {
            valueMap.remove(removedValue);
        }
        return removedValue;
    }

    @Override
    public boolean containsValue(Object value) {
        // 检查 valueMap 是否包含此值
        return valueMap.containsKey(value);
    }

    public Long getKeyFromValue(String value) {
        // 从 valueMap 中获取键
        return valueMap.get(value);
    }

    // 新增方法：删除所有键小于给定 time 的记录
    public void removeKeysLessThan(Long time) {
        // 获取所有小于给定时间的键的子映射
        ConcurrentNavigableMap<Long, String> headMap = headMap(time, false);

        // 遍历并删除键和值
        for (Long key : headMap.keySet()) {
            String removedValue = remove(key);
            if (removedValue != null) {
                valueMap.remove(removedValue);
            }
        }
    }
}
