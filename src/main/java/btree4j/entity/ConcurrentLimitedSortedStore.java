package btree4j.entity;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;
/*
①支持kv，k是String，v是Long
②支持以v进行排序
③能够在常数级别判断k是否存在。
④高并发下安全
⑤有数量限制，如果到达阈值，则丢弃value最小的值
 */
public class ConcurrentLimitedSortedStore {

    private static class Entry implements Comparable<Entry> {
        String key;
        Long value;

        Entry(String key, Long value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public int compareTo(Entry other) {
            int valueComparison = this.value.compareTo(other.value); // 按着时间升序
            if (valueComparison != 0) {
                return valueComparison;
            }
            return this.key.compareTo(other.key); // 防止 value 相同时 key 冲突
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null || getClass() != obj.getClass())
                return false;
            Entry entry = (Entry) obj;
            return Objects.equals(key, entry.key) && Objects.equals(value, entry.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value);
        }
    }

    private final ConcurrentHashMap<String, Long> map; // 存储 key-value 对
    private final ConcurrentSkipListSet<Entry> sortedSet; // 按 value 排序
    private final int maxSize; // 最大容量限制
    private final ReentrantLock lock; // 写锁

    public ConcurrentLimitedSortedStore(int maxSize) {
        this.map = new ConcurrentHashMap<>();
        this.sortedSet = new ConcurrentSkipListSet<>();
        this.maxSize = maxSize;
        this.lock = new ReentrantLock();
    }

    // 插入或更新
    public void put(String key, Long value) {
        lock.lock();
        try {
            if (map.containsKey(key)) {
                // 如果 key 存在，先移除旧的 Entry
                Long oldValue = map.get(key);
                sortedSet.remove(new Entry(key, oldValue));
            }

            // 插入新的 Entry
            map.put(key, value);
            sortedSet.add(new Entry(key, value));

            // 如果超过最大容量，移除最小值
            if (map.size() > maxSize) {
                Entry smallestEntry = sortedSet.pollFirst(); // 移除最小值
                if (smallestEntry != null) {
                    map.remove(smallestEntry.key);
                }
            }
        } finally {
            lock.unlock();
        }
    }

   
    // 删除
    public void remove(String key) {
        lock.lock();
        try {
            if (map.containsKey(key)) {
                Long value = map.get(key);
                sortedSet.remove(new Entry(key, value));
                map.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }

    // 判断是否包含 key
    public boolean containsKey(String key) {
        return map.containsKey(key);
    }

    

    // 获取按 value 排序的前 n 个条目
    public List<Map.Entry<String, Long>> topN(int n) {
        List<Map.Entry<String, Long>> result = new ArrayList<>();
        int count = 0;
        for (Entry entry : sortedSet) {
            if (count >= n)
                break;
            result.add(new AbstractMap.SimpleEntry<>(entry.key, entry.value));
            count++;
        }
        return result;
    }

    public int size() {
        return map.size();
    }

    // 按照顺序获取所有条目
    public Set<Map.Entry<String, Long>> entrySet() {
        // 创建快照
        List<Entry> snapshot = new ArrayList<>(sortedSet);

        // 生成有序结果
        Set<Map.Entry<String, Long>> result = new LinkedHashSet<>();
        for (Entry entry : snapshot) {
            result.add(new AbstractMap.SimpleEntry<>(entry.key, entry.value));
        }
        System.out.println(result);;
        return result;
    }

    public void removeKeysLessThan(String key) {
        lock.lock();
        try {
            Long referenceValue = map.get(key);
            if (referenceValue == null) {
                throw new IllegalArgumentException("Key not found: " + key);
            }
    
            // 遍历 sortedSet，移除所有 value 小于 referenceValue 的 Entry
            Iterator<Entry> iterator = sortedSet.iterator();
            while (iterator.hasNext()) {
                Entry entry = iterator.next();
                if (entry.value < referenceValue) {
                    iterator.remove();       // 从 sortedSet 移除
                    map.remove(entry.key);   // 从 map 移除
                }
            }
        } finally {
            lock.unlock();
        }
    }
    

}