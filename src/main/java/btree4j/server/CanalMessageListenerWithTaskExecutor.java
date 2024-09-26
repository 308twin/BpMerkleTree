package btree4j.server;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;

import btree4j.service.CompareService;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.net.InetSocketAddress;
import java.util.concurrent.Executor;
import btree4j.utils.*;;
@Component
public class CanalMessageListenerWithTaskExecutor {

    @Value("${canal.server}")
    private String canalServer;

    @Value("${canal.port}")
    private int canalPort;

    @Value("${canal.destination}")
    private String destination;

    @Value("${canal.username:}")
    private String username;

    @Value("${canal.password:}")
    private String password;

    @Value("${canal.subscription:.*\\..*}")
    private String subscription;

    private CanalConnector connector;
    private volatile boolean running = true;

    private final Executor canalTaskExecutor;
    private CompareService  compareService;

    public CanalMessageListenerWithTaskExecutor(Executor canalTaskExecutor, CompareService compareService) {
        this.canalTaskExecutor = canalTaskExecutor;
        this.compareService = compareService;
    }

    @PostConstruct
    public void start() {
        // 初始化连接器
        connector = CanalConnectors.newSingleConnector(
                new InetSocketAddress(canalServer, canalPort),
                destination,
                username,
                password
        );

        // 启动监听线程
        new Thread(this::processMessages).start();
    }

    private void processMessages() {
        try {
            connector.connect();
            connector.subscribe(subscription);
            connector.rollback(); // 回滚到上一次未处理的位置

            while (running) {
                // 获取消息，批量获取100条
                Message message = connector.getWithoutAck(1000, 100L, java.util.concurrent.TimeUnit.MILLISECONDS);
                long batchId = message.getId();
                int size = message.getEntries().size();

                if (batchId == -1 || size == 0) {
                    Thread.sleep(1000); // 没有新消息，稍作休眠
                } else {
                    // 将消息提交给线程池处理
                    for (CanalEntry.Entry entry : message.getEntries()) {
                        if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
                            canalTaskExecutor.execute(() -> handleEntry(entry));
                        }
                    }
                    // 提交确认
                    connector.ack(batchId);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            connector.disconnect();
        }
    }

    private void handleEntry(CanalEntry.Entry entry) {
        try {
            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            String dbName = entry.getHeader().getSchemaName(); // 数据库名
            String tableName = entry.getHeader().getTableName(); // 表名

            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                if (rowChange.getEventType() == CanalEntry.EventType.INSERT) {
                    String primaryKey = null;
                    String update_time_on_chain = null;
                    for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                        if (column.getIsKey()) {
                            primaryKey = column.getValue(); // 主键字段
                        }
                        if ("update_time_on_chain".equals(column.getName())) {
                            update_time_on_chain = column.getValue(); // 插入时间,2024-09-23 15:30:31.000
                        }
                    }
                    // 输出数据库名、表名、主键字段、插入时间
                    System.out.println("数据库名: " + dbName);
                    System.out.println("表名: " + tableName);
                    System.out.println("主键字段: " + primaryKey);
                    System.out.println("插入时间: " + update_time_on_chain);
                    compareService.addRecordToInsertRecord(dbName + "__" + tableName,Utils.convertStringToLong(update_time_on_chain)  , primaryKey);
                }else if (rowChange.getEventType() == CanalEntry.EventType.DELETE) {
                    String primaryKey = null;
                    long deleteTime = entry.getHeader().getExecuteTime(); // 删除事件的执行时间
                    for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
                        if (column.getIsKey()) {
                            primaryKey = column.getValue(); // 获取主键的值
                        }
                    }
                    // 输出数据库名、表名、主键字段、删除时间
                    System.out.println("数据库名: " + dbName);
                    System.out.println("表名: " + tableName);
                    System.out.println("主键字段: " + primaryKey);
                    System.out.println("删除时间: " + String.valueOf(deleteTime));
                
                } else if (rowChange.getEventType() == CanalEntry.EventType.CREATE) {
                    // CREATE事件只需表名
                    System.out.println("表创建: " + tableName);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @PreDestroy
    public void stop() {
        running = false;
        if (connector != null) {
            connector.disconnect();
        }
    }
}
