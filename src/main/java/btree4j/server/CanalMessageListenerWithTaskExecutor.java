package btree4j.server;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import btree4j.BTree;
import btree4j.entity.TypeWithTime;
import btree4j.service.CompareService;
import btree4j.service.MqService;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import btree4j.utils.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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

    @Value("${canal.subscription}")
    private String subscription;

    private CanalConnector connector;
    private volatile boolean running = true;

    private final Executor canalTaskExecutor;
    private CompareService compareService;
    private MqService mqService;

    private static final Log LOG = LogFactory.getLog(CanalMessageListenerWithTaskExecutor.class);

    // 全局共享的 ObjectMapper，经过配置后线程安全
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .disable(SerializationFeature.INDENT_OUTPUT) // 不格式化，确保无额外空白
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS); // 确保 Map 按 key 排序

    // 使用 ThreadLocal 缓存 TreeMap 实例，避免每次 new TreeMap()
    private static final ThreadLocal<TreeMap<String, String>> threadLocalTreeMap = ThreadLocal
            .withInitial(TreeMap::new);

    public CanalMessageListenerWithTaskExecutor(Executor canalTaskExecutor, CompareService compareService,
             MqService mqService) {
        this.canalTaskExecutor = canalTaskExecutor;
        this.compareService = compareService;
        this.mqService = mqService;
    }

    @PostConstruct
    public void start() {
        // 初始化连接器
        connector = CanalConnectors.newSingleConnector(
                new InetSocketAddress(canalServer, canalPort),
                destination,
                username,
                password);

        // 启动监听线程
        new Thread(this::processMessages).start();
    }

    private void processMessages() {
        try {
            connector.connect();
            connector.subscribe(subscription);
            connector.rollback(); // 回滚到上一次未处理的位置

            while (running) {
                // 获取消息，批量获取1000条
                Message message = connector.getWithoutAck(1000, 100L, java.util.concurrent.TimeUnit.MILLISECONDS);
                long batchId = message.getId();
                int size = message.getEntries().size();

                if (batchId == -1 || size == 0) {
                    Thread.sleep(1000); // 没有新消息，稍作休眠
                } else {
                    // 使用 CountDownLatch 等待所有任务完成
                    CountDownLatch latch = new CountDownLatch(message.getEntries().size());
                    for (CanalEntry.Entry entry : message.getEntries()) {
                        if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
                            canalTaskExecutor.execute(() -> {
                                try {
                                    handleBDCEntry(entry);
                                } finally {
                                    latch.countDown();
                                }
                            });
                        } else {
                            latch.countDown(); // 如果不是 ROWDATA 类型，也要减少计数
                        }
                    }
                    // 等待所有任务完成
                    latch.await();
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

    /*
     * CREATE TABLE `supervise_online_vehicle` (
     * `block` int NOT NULL,
     * `tx_num` int NOT NULL,
     * `chain_id` varchar(255) DEFAULT NULL,
     * `tx_id` varchar(255) DEFAULT NULL,
     * `operate_time` bigint DEFAULT NULL,
     * `contract_name` varchar(255) DEFAULT NULL,
     * `version` varchar(50) DEFAULT NULL,
     * `user_id` varchar(255) DEFAULT NULL,
     * `method` varchar(255) DEFAULT NULL,
     * `app_code` varchar(255) DEFAULT NULL,
     * `rule_id` varchar(255) DEFAULT NULL,
     * `raw_data` json DEFAULT NULL,
     * `signature` varchar(255) DEFAULT NULL,
     * `verify_hash` varchar(255) DEFAULT NULL,
     * PRIMARY KEY (`block`,`tx_num`)
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
     * 以上是 BDC 存储的通用格式，其中 block 和 tx_num 为主键
     */
    private void handleBDCEntry(CanalEntry.Entry entry) {
        try {
            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            String dbName = entry.getHeader().getSchemaName(); // 数据库名
            String tableName = entry.getHeader().getTableName(); // 表名
            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                // 获取线程内复用的 TreeMap 并清空旧数据
                TreeMap<String, String> canonicalJsonMap = threadLocalTreeMap.get();
                canonicalJsonMap.clear();
                String tx_id = null;
                String block = null;
                String tx_num = null;

                // 这里可以排除 signature、verify_hash 等字段
                for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                    String columnName = column.getName();
                    if ("signature".equalsIgnoreCase(columnName) || "verify_hash".equalsIgnoreCase(columnName)) {
                        continue;
                    }
                    if ("tx_id".equalsIgnoreCase(columnName)) {
                        tx_id = column.getValue();
                    }
                    if ("block".equalsIgnoreCase(columnName)) {
                        block = column.getValue();
                    }
                    if ("tx_num".equalsIgnoreCase(columnName)) {
                        tx_num = column.getValue();
                    }
                    // 统一转换字段值
                    canonicalJsonMap.put(columnName, normalizeValue(column.getValue()));
                    /*
                     * example:{"app_code":"xdwycxt","block":"17","chain_id":"ZW_C8458A2_004",
                     * "contract_name":"SuperviseOnlineVehicle","method":"CreateHistoryVehicle",
                     * "operate_time":"1723602624",
                     * "raw_data":"[{\"bizData\": {\"Ukey\": \"ukey001\", \"UuId\": \"car1234567\", \"Status\": 0, \"AddTime\": \"2024-06-24 16:55:05\", \"UpdTime\": \"2024-06-24 16:55:05\", \"IsRemove\": \"Y\", \"RegisterDate\": \"2024-06-25 09:09:09\"}, \"bizHash\": \"59edbe55748e456194a6a5755f6415c2\", \"bizIndex\": \"230423162136mEefCn9OLj6A7DvR8St12\"}]"
                     * ,"rule_id":"240624132525HczjzQYHJfeyIxyARQP","tx_id":
                     * "17eb77747bc93ca8cafbe27e7458093da8b6f3746e0342df86f4b634f1f3cf90","tx_num":
                     * "1","user_id":"","version":"V1.0"}
                     */
                }
                Long blockAndTx = Long.parseLong(block) * 100000 + Long.parseLong(tx_num);
                
                // 生成 Canonical JSON 字符串，ObjectMapper 会保证字段顺序和格式一致
                String canonicalJson = objectMapper.writeValueAsString(canonicalJsonMap);
                if (rowChange.getEventType() == CanalEntry.EventType.INSERT) {
                    compareService.addToLocalBinRecords(dbName, tableName, tx_id, new TypeWithTime( //这里为了方便，直接复用了TypeWithTime,使用blockAndTx代替time
                        blockAndTx,TypeWithTime.OperationType.INSERT));
                    // 插入操作是先插入到待插入列表，然后再插入到btree中
                    compareService.addRecordToInsertRecord(dbName + "__" + tableName,
                            blockAndTx, tx_id);
                    mqService.sendSignatureToRemote(canonicalJson, tx_id, tableName);
                }
                else if (rowChange.getEventType() == CanalEntry.EventType.DELETE) {
                    compareService.addToLocalBinRecords(dbName, tableName, tx_id, new TypeWithTime(
                            blockAndTx,TypeWithTime.OperationType.DELETE));
                    // 由于删除操作很少进行，所以直接删除
                    String newestHash = compareService.removeKeyFromBtree(dbName + "__" +
                            tableName, tx_id, blockAndTx);
                    if (!newestHash.equals(compareService.newestHashAfterRemove)) // 如果删除后的hash和删除前的hash不一样,则更新。否则会导致错误
                        compareService.insertHashToLocalHashs(dbName + "__" + tableName, newestHash);
                }
                // 输出或后续处理：可以用于签名、存储、日志打印等
                //System.out.println("数据库：" + dbName + "，表：" + tableName + "，Canonical JSON：" + canonicalJson);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 统一数据格式化方法，确保相同数据转换后得到的字符串一致。
     * 可以根据实际数据类型扩展此方法，比如日期、数字的格式转换。
     */
    private String normalizeValue(String value) {
        if (value == null) {
            return "";
        }
        // 根据需要对数值、日期等进行格式化处理
        return value.trim();
    }
    

    @PreDestroy
    public void stop() {
        running = false;
        if (connector != null) {
            connector.disconnect();
        }
    }
}
