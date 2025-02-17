package btree4j.server;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Component
public class SignatureUpdateQueue {
    private static final Log LOG = LogFactory.getLog(SignatureUpdateQueue.class);
    
    private final Queue<SignatureUpdate> pendingUpdates = new ConcurrentLinkedQueue<>();
    private final Queue<SignatureUpdate> delayedUpdates = new ConcurrentLinkedQueue<>();
    private final JdbcTemplate jdbcTemplate;
    private final ScheduledExecutorService scheduler;
    private static final int BATCH_SIZE = 100;
    private static final int MAX_RETRY = 10;
    private static final int RETRY_DELAY_SECONDS = 30;

    public SignatureUpdateQueue(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.scheduler = Executors.newScheduledThreadPool(2);
        initSchedulers();
    }

    private void initSchedulers() {
        // 定期处理待更新队列
        scheduler.scheduleAtFixedRate(this::processPendingUpdates, 5, 5, TimeUnit.SECONDS);
        // 定期处理延迟队列
        scheduler.scheduleAtFixedRate(this::processDelayedUpdates, 10, 10, TimeUnit.SECONDS);
    }

    public void addUpdate(String signature, String txId, String tableName) {
        pendingUpdates.offer(new SignatureUpdate(signature, txId, tableName));
    }

    private void processPendingUpdates() {
        List<SignatureUpdate> batch = new ArrayList<>();
        while (!pendingUpdates.isEmpty() && batch.size() < BATCH_SIZE) {
            batch.add(pendingUpdates.poll());
        }

        if (batch.isEmpty()) return;

        // 按表名分组，减少查询次数
        Map<String, List<SignatureUpdate>> updatesByTable = batch.stream()
                .collect(Collectors.groupingBy(update -> update.tableName));

        for (Map.Entry<String, List<SignatureUpdate>> entry : updatesByTable.entrySet()) {
            String tableName = entry.getKey();
            List<SignatureUpdate> updates = entry.getValue();
            
            // 构建批量查询SQL
            String txIds = updates.stream()
                    .map(u -> "'" + u.txId + "'")
                    .collect(Collectors.joining(","));
            
            String checkSql = String.format("SELECT tx_id FROM %s WHERE tx_id IN (%s)", tableName, txIds);
            
            try {
                // 一次性查询所有存在的记录
                Set<String> existingTxIds = jdbcTemplate.query(checkSql, (rs, rowNum) -> rs.getString("tx_id"))
                        .stream()
                        .collect(Collectors.toSet());

                // 构建批量更新
                List<Object[]> batchArgs = new ArrayList<>();
                List<SignatureUpdate> delayedUpdates = new ArrayList<>();

                for (SignatureUpdate update : updates) {
                    if (existingTxIds.contains(update.txId)) {
                        batchArgs.add(new Object[]{update.signature, update.txId});
                    } else {
                        update.lastRetryTime = System.currentTimeMillis();
                        delayedUpdates.add(update);
                    }
                }

                // 执行批量更新
                if (!batchArgs.isEmpty()) {
                    String updateSql = String.format("UPDATE %s SET signature = ? WHERE tx_id = ?", tableName);
                    jdbcTemplate.batchUpdate(updateSql, batchArgs);
                    LOG.info("Batch updated " + batchArgs.size() + " signatures for table " + tableName);
                }

                // 处理不存在的记录
                if (!delayedUpdates.isEmpty()) {
                    this.delayedUpdates.addAll(delayedUpdates);
                    LOG.info(delayedUpdates.size() + " records not found for table " + tableName + ", moved to delayed queue");
                }

            } catch (Exception e) {
                LOG.error("Error processing signature updates for table " + tableName, e);
                // 发生错误时，将所有更新移到延迟队列
                updates.forEach(update -> {
                    update.lastRetryTime = System.currentTimeMillis();
                    this.delayedUpdates.offer(update);
                });
            }
        }
    }

    private void processDelayedUpdates() {
        List<SignatureUpdate> toRetry = new ArrayList<>();
        long now = System.currentTimeMillis();

        while (!delayedUpdates.isEmpty()) {
            SignatureUpdate update = delayedUpdates.poll();
            if (update == null) continue;

            // 检查是否应该重试
            if (now - update.lastRetryTime >= TimeUnit.SECONDS.toMillis(RETRY_DELAY_SECONDS)) {
                if (update.retryCount < MAX_RETRY) {
                    update.retryCount++;
                    update.lastRetryTime = now;
                    toRetry.add(update);
                } else {
                    LOG.error("Max retry exceeded for signature update: " + update.txId);
                }
            } else {
                // 还没到重试时间，放回队列
                delayedUpdates.offer(update);
            }
        }

        // 将需要重试的更新放回待处理队列
        pendingUpdates.addAll(toRetry);
    }

    private static class SignatureUpdate {
        final String signature;
        final String txId;
        final String tableName;
        int retryCount = 0;
        long lastRetryTime;

        SignatureUpdate(String signature, String txId, String tableName) {
            this.signature = signature;
            this.txId = txId;
            this.tableName = tableName;
            this.lastRetryTime = System.currentTimeMillis();
        }
    }
}
