package btree4j.entity;

import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Iterator;

public class LimitedLinkedHashSet<T> extends LinkedHashSet<T> {
    private final int maxCapacity;
    private final LinkedList<T> orderList;

    public LimitedLinkedHashSet(int maxCapacity) {
        super(maxCapacity);
        this.maxCapacity = maxCapacity;
        this.orderList = new LinkedList<>();
    }

    @Override
    public boolean add(T element) {
        // 容量超限，移除最早的元素
        if (this.size() >= maxCapacity) {
            T oldest = orderList.removeFirst();
            super.remove(oldest);
        }
        // 如果元素已存在，避免重复添加
        if (super.add(element)) {
            orderList.addLast(element); // 插入到末尾
            return true;
        }
        return false;
    }

    @Override
    public boolean remove(Object element) {
        orderList.remove(element); // 同步移除LinkedList中的元素
        return super.remove(element);
    }

    @Override
    public void clear() {
        orderList.clear(); // 清空LinkedList
        super.clear();
    }

    // 从最新到最旧的顺序遍历
    public Iterator<T> reverseIterator() {
        return orderList.descendingIterator();
    }
}
