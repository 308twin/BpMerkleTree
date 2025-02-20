package btree4j.server;
import java.util.List;
import java.util.Queue;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.jdbc.core.JdbcTemplate;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
@Service
public class DBService {
    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private SignatureUpdateQueue signatureUpdateQueue;
    // 用于缓存SQL语句的线程安全队列
    private final Queue<String> sqlCache = new ConcurrentLinkedQueue<>();
    // 用于存储执行失败的SQL语句的线程安全队列
    private final Queue<FailedSQL> failedSqlCache = new ConcurrentLinkedQueue<>();
    private final int BATCH_SIZE = 10000;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static final Log logger = LogFactory.getLog(DBService.class);

    // 执行查询来获取所有的表名
    public List<String> getAllTableNames() {
        String sql = "SHOW TABLES";
        return jdbcTemplate.queryForList(sql, String.class);
    }

    // 初始化调度程序，每隔5秒执行一次批量操作
    public DBService() {
        scheduler.scheduleAtFixedRate(this::executeBatch, 5, 5, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(this::retryFailedBatch, 5, 5, TimeUnit.MINUTES);
    }

    // 添加SQL语句到缓存中
    public void addSQL(String sql) {
        sqlCache.add(sql);
        // 如果缓存达到批量大小，立即执行批处理
        if (sqlCache.size() >= BATCH_SIZE) {
            executeBatch();
        }
    }

    // 批量执行缓存的SQL语句
    private void executeBatch() {
        List<String> batch = new ArrayList<>();
        while (!sqlCache.isEmpty() && batch.size() < BATCH_SIZE) {
            batch.add(sqlCache.poll());
        }
        if (!batch.isEmpty()) {
            try {
                jdbcTemplate.batchUpdate(batch.toArray(new String[0]));
                logger.info("Batch executed with " + batch.size() + " statements.");
            } catch (Exception e) {
                logger.info("Error executing batch: " + e.getMessage());
                // 将失败的SQL添加到失败缓存中并记录失败次数
                for (String sql : batch) {
                    failedSqlCache.add(new FailedSQL(sql));
                }
            }
        }
    }

    // 重试执行失败的SQL语句
    private void retryFailedBatch() {
        List<FailedSQL> batch = new ArrayList<>();
        while (!failedSqlCache.isEmpty() && batch.size() < BATCH_SIZE) {
            batch.add(failedSqlCache.poll());
        }
        if (!batch.isEmpty()) {
            List<String> sqlStatements = new ArrayList<>();
            for (FailedSQL failedSQL : batch) {
                if (failedSQL.getRetryCount() < 10) {
                    sqlStatements.add(failedSQL.getSql());
                }
            }
            if (!sqlStatements.isEmpty()) {
                try {
                    jdbcTemplate.batchUpdate(sqlStatements.toArray(new String[0]));
                    logger.info("Retry batch executed with " + sqlStatements.size() + " statements.");
                } catch (Exception e) {
                    logger.info("Error retrying batch: " + e.getMessage());
                    // 记录重试失败的SQL语句，增加其重试次数
                    for (FailedSQL failedSQL : batch) {
                        failedSQL.incrementRetryCount();
                        if (failedSQL.getRetryCount() < 10) {
                            failedSqlCache.add(failedSQL);
                        } else {
                            logger.error("Max retry limit reached for SQL: " + failedSQL.getSql());
                        }
                    }
                }
            }
        }
    }

    /**
     * 根据交易ID查询记录
     * @param tableName 表名
     * @param txId 交易ID
     * @return 包含所有字段的Map，如果未找到则返回null
     */
    public Map<String, Object> getRecordByTxId(String tableName, String txId) {
        String sql = String.format("SELECT * FROM %s WHERE tx_id = ?", tableName);
        try {
            List<Map<String, Object>> results = jdbcTemplate.queryForList(sql, txId);
            return results.isEmpty() ? null : results.get(0);
        } catch (Exception e) {
            logger.error("查询交易记录失败: " + e.getMessage(), e);
            return null;
        }
    }

    public void updateSignature(String signature, String txId, String tableName) {
        signatureUpdateQueue.addUpdate(signature, txId, tableName);
    }
    // 内部类用于存储失败的SQL和重试次数
    private static class FailedSQL {
        private final String sql;
        private int retryCount;

        public FailedSQL(String sql) {
            this.sql = sql;
            this.retryCount = 0;
        }

        public String getSql() {
            return sql;
        }

        public int getRetryCount() {
            return retryCount;
        }

        public void incrementRetryCount() {
            this.retryCount++;
        }
    }
    
}
