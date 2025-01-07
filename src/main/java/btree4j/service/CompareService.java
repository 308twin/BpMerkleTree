package btree4j.service;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Service;
import org.springframework.jdbc.core.JdbcTemplate;
import btree4j.BTree;
import btree4j.BTreeException;
import btree4j.Value;
import btree4j.entity.LimitedLinkedHashSet;
import btree4j.entity.LimitedSizeConcurrentSkipListMapDescending;
import btree4j.entity.ConcurrentLimitedSortedStore;
import btree4j.entity.LimitedLinkedHashSet;
import btree4j.entity.MerkleHashEntity;
import btree4j.entity.TypeWithTime;
import btree4j.utils.Utils;
import btree4j.utils.io.FileUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import com.google.common.cache.CacheBuilder;
//import com.esotericsoftware.minlog.Log;
import com.google.common.cache.Cache;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

@Service
@ConfigurationProperties(prefix = "my.custom.config")
public class CompareService {
    private static final Log LOG = LogFactory.getLog(CompareService.class);

    @org.springframework.beans.factory.annotation.Value("${my.custom.config.timeFram}")
    private String timeFram;

    @org.springframework.beans.factory.annotation.Value("${my.custom.config.localHashMapMaxSize}")
    private int localHashMapMaxSize;

    @org.springframework.beans.factory.annotation.Value("${spring.datasource.url}")
    private String url;

    @org.springframework.beans.factory.annotation.Value("${my.custom.config.isServer}")
    private boolean isServer;

    @org.springframework.beans.factory.annotation.Value("${my.custom.config.strictMode}")
    private boolean strictMode;

    public String newestHashAfterRemove; // 删除key之后的最新hash 之所以要记录这个是因为删除不存在key之后 root hash可能不变化
    private ConcurrentHashMap<String, ConcurrentLimitedSortedStore> localHashs;
    private ConcurrentHashMap<String, ConcurrentLimitedSortedStore> remoteHashs;
    private ConcurrentHashMap<String, ConcurrentLimitedSortedStore> aboutToSendHashs;
    private ConcurrentHashMap<String, Boolean> isConcistByMerkleHash;
    private ConcurrentHashMap<String, BTree> localBTrees;
    private ConcurrentHashMap<String, ConcurrentLimitedSortedStore> aboutToInsertRecord;
    private ConcurrentHashMap<String, ConcurrentHashMap<String, TypeWithTime>> remoteBinRecords;
    private ConcurrentHashMap<String, ConcurrentHashMap<String, TypeWithTime>> localBinRecords;
    private ConcurrentHashMap<String, Boolean> isConcistByRecord;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    public CompareService(@Qualifier("localHashs") ConcurrentHashMap<String, ConcurrentLimitedSortedStore> localHashs,
            @Qualifier("remoteHashs") ConcurrentHashMap<String, ConcurrentLimitedSortedStore> remoteHashs,
            @Qualifier("aboutToSendHashs") ConcurrentHashMap<String, ConcurrentLimitedSortedStore> aboutToSendHashs,
            ConcurrentHashMap<String, BTree> localBTrees,
            ConcurrentHashMap<String, ConcurrentLimitedSortedStore> aboutToInsertRecord,
            ConcurrentHashMap<String, Boolean> isConcistByMerkleHash,
            @Qualifier("remoteBinRecords") ConcurrentHashMap<String, ConcurrentHashMap<String, TypeWithTime>> remoteBinRecords,
            @Qualifier("localBinRecords") ConcurrentHashMap<String, ConcurrentHashMap<String, TypeWithTime>> localBinRecords,
            ConcurrentHashMap<String, Boolean> isConcistByRecord) {
        this.localHashs = localHashs;
        this.remoteHashs = remoteHashs;
        this.aboutToSendHashs = aboutToSendHashs;
        this.localBTrees = localBTrees;
        this.aboutToInsertRecord = aboutToInsertRecord;
        this.isConcistByMerkleHash = isConcistByMerkleHash;
        this.remoteBinRecords = remoteBinRecords;
        this.localBinRecords = localBinRecords;
        this.isConcistByRecord = isConcistByRecord;
    }

    // get btree by key ,if not exist ,create a new btree,if exist return the
    // btree(from disk or memory)
    public BTree getBTree(String dbAndTable) throws BTreeException {
        BTree btree = localBTrees.get(dbAndTable);
        if (btree == null) {
            File tmpDir = FileUtils.getFileDir();
            File tmpFile = new File(tmpDir, dbAndTable + "BTree.idx");
            btree = new BTree(tmpFile);
            btree.init(/* bulkload */ true);
            localBTrees.put(dbAndTable, btree);
        }
        return btree;
    }

    /*
     * 将最新生成的hash插入到localHashs中，如果localHashs中没有dbAndTable对应的hash列表，则创建一个新的hash列表
     */
    public void insertHashToLocalHashs(String dbAndTable, String hash) {
        LOG.debug("insertHashToLocalHashs dbAndTable:" + dbAndTable + ",hash:" + hash);
        ConcurrentLimitedSortedStore tableHashHistorys = localHashs.computeIfAbsent(dbAndTable,
                k -> new ConcurrentLimitedSortedStore(localHashMapMaxSize));

        ConcurrentLimitedSortedStore aboutToSendHistorys = aboutToSendHashs.computeIfAbsent(dbAndTable,
                k -> new ConcurrentLimitedSortedStore(localHashMapMaxSize));

        long time = System.currentTimeMillis();
        tableHashHistorys.put(hash, time);
        aboutToSendHistorys.put(hash, time);
    }

    public synchronized void insertKeyToBtree(String dbAndTable, String value, long time) throws BTreeException {
        BTree btree = getBTree(dbAndTable);
        Value k = new Value(value);
        btree.addValue(k, time);
        System.out.println("Success insert key:" + value + ",newest root hash is : " + btree.getRootMerkleHash());
    }

    public synchronized String removeKeyFromBtree(String dbAndTable, String value, long time) throws BTreeException {
        BTree btree = getBTree(dbAndTable);
        Value k = new Value(value);
        btree.removeValue(k, time); // 这个found没用
        String newestHash = btree.getRootMerkleHash();
        this.newestHashAfterRemove = newestHash;
        System.out.println("Success remove key:" + value + ",newest root hash is : " + newestHash);
        return newestHash;
    }

    public String getBTreeRootMerkleHash(String dbAndTable) throws BTreeException {
        BTree btree = getBTree(dbAndTable);
        return btree.getRootMerkleHash();
    }

    // 将记录插入到待插入列表，使用concurrentSkipListMap存储,排序方式是按照时间戳排序
    public synchronized void addRecordToInsertRecord(String dbAndTable, long time, String value) {
        ConcurrentLimitedSortedStore valueMap = aboutToInsertRecord.computeIfAbsent(
                dbAndTable,
                k -> new ConcurrentLimitedSortedStore(Integer.MAX_VALUE));
        valueMap.put(value, time);
    }

    public void addToRemoteHashs(String dbAndTable, long time, String hash) {
        ConcurrentLimitedSortedStore remoteHashsSet = remoteHashs.computeIfAbsent(
                dbAndTable,
                k -> new ConcurrentLimitedSortedStore(localHashMapMaxSize));
        remoteHashsSet.put(hash, time);
    }

    // 通过remoteHashs中的hash和localHashs中的hash进行比较，如果不一致则返回false
    // 判断的范围是根据localHashs的全部内容
    // 具体比较规则：获取dbAndTable对应的localHashs和remoteHashs，遍历localHashs，从更大的时间开始往前比较，如果存在某个hash一致，则更新isConcistByMerkleHash对应的值为true
    // 如果比较到timeFram之前的时间都没有一致的hash，则更新isConcistByMerkleHash对应的值为false
    // 如果remoteHashs中的hash和localHashs中的hash都为空，则不修改isConcistByMerkleHash对应的值
    // 最后，把localHashs和remoteHashs中的数据清空
    public void isConcistByMerkleHash(String dbAndTable) {
        ConcurrentLimitedSortedStore localHashsMap = localHashs
                .get(dbAndTable);
        ConcurrentLimitedSortedStore remoteHashsMap = remoteHashs
                .get(dbAndTable);
        if ((localHashsMap == null || localHashsMap.size() == 0)
                && (remoteHashsMap == null || remoteHashsMap.size() == 0)) {
            // System.out.println("localHashsMap is null and remoteHashsMap is null");
            isConcistByMerkleHash.put(dbAndTable, true);
            return;
        }
        if (localHashsMap.size() != 0
                && (remoteHashsMap == null || remoteHashsMap.size() == 0)) {
            // System.out.println("localHashsMap is not null and remoteHashsMap is null");
            isConcistByMerkleHash.put(dbAndTable, false);
            return;
        }

        if ((localHashsMap == null || localHashsMap.size() == 0)
                && remoteHashsMap.size() != 0) {
            // System.out.println("localHashsMap is null and remoteHashsMap is not null");
            // print remote
            for (Map.Entry<String, Long> entry : remoteHashsMap.entrySet()) {
                // System.out.println("time:" + entry.getKey() + ",hash:" + entry.getValue());
            }
            isConcistByMerkleHash.put(dbAndTable, false);
            return;
        }

        boolean isConcist = false;

        String newestConcistHash = null;

        // 从大到小遍历localHashs,如果localHashs中的hash存在于remoteHashs中，则更新isConcist为true
        if (localHashsMap != null && remoteHashsMap != null) {
            System.out.println("localHashsMap is not null and remoteHashsMap is not null");
            for (Map.Entry<String, Long> entry : localHashsMap.reverseEntrySet()) {
                //Long time = entry.getValue();
                String localHash = entry.getKey();
                // 在remoteHashs中从后往前查找localHash
                if (remoteHashsMap.containsKey(localHash)) {
                    isConcist = true;
                    newestConcistHash = localHash;
                    break;
                }

            }
        }
        // 删除localHashs和remoteHashs中的在newestConcistHash之前插入的数据
        if (isConcist) {
            System.out.println("delete keys less than newestConcistHash:" + newestConcistHash);
            remoteHashsMap.removeKeysLessThan(newestConcistHash);
            localHashsMap.removeKeysLessThan(newestConcistHash);

            remoteHashsMap.remove(newestConcistHash);
            localHashsMap.remove(newestConcistHash);

        }
        isConcistByMerkleHash.put(dbAndTable, isConcist);
    }

    public synchronized void addToLocalBinRecords(String dbName, String tableName, String key,
            TypeWithTime typeWithTime) {
        String dbAndTable = dbName + "__" + tableName;
        localBinRecords
                .computeIfAbsent(dbAndTable, k -> new ConcurrentHashMap<>())
                .put(key, typeWithTime);
    }

    public void addToRemoteBinRecords(String dbName, String tableName, String key, TypeWithTime typeWithTime) {
        String dbAndTable = dbName + "__" + tableName;
        remoteBinRecords
                .computeIfAbsent(dbAndTable, k -> new ConcurrentHashMap<>())
                .put(key, typeWithTime);
    }

    public void matchAllRecords() {
        List<String> dbAndTables = new ArrayList<>();
        dbAndTables.addAll(localBinRecords.keySet());
        for (String dbAndTable : dbAndTables) {
            isConcistByRecord(dbAndTable);
        }
    }

    public void matchAllHashs() {
        List<String> dbAndTables = new ArrayList<>();
        dbAndTables.addAll(localHashs.keySet());
        for (String dbAndTable : dbAndTables) {
            isConcistByMerkleHash(dbAndTable);
        }
    }

    /*
     * 如果remoteBinRecords中的记录和localBinRecords中的记录为空，则更新isConcistByRecord对应的值为true
     * 如果remoteBinRecords中的最旧的记录和localBinRecords中最旧的记录有时间(超过当前时间-timeFram)的记录，
     * 则更新isConcistByRecord对应的值为false
     */
    public void isConcistByRecord(String dbAndTable) {
        ConcurrentHashMap<String, TypeWithTime> localRecords = localBinRecords.get(dbAndTable);
        ConcurrentHashMap<String, TypeWithTime> remoteRecords = remoteBinRecords.get(dbAndTable);

        if ((localRecords == null || localRecords.size() == 0)
                && (remoteRecords == null || remoteRecords.size() == 0)) {
            isConcistByRecord.put(dbAndTable, true);
            return;
        }

        if ((localRecords == null || localRecords.size() == 0)
                && remoteRecords.size() != 0) {
            isConcistByRecord.put(dbAndTable, false);
            return;
        }

        if ((remoteRecords == null || remoteRecords.size() == 0)
                && localRecords.size() != 0) {
            isConcistByRecord.put(dbAndTable, false);
            return;
        }

        // 遍历localRecords,如果localRecord中的TypeWithTime的值存在于remoteRecords中，则删除localRecord中的TypeWithTime，以及remoteRecords中的TypeWithTime
        if (localRecords != null && remoteRecords != null) {
            for (Map.Entry<String, TypeWithTime> entry : localRecords.entrySet()) {
                String key = entry.getKey();
                TypeWithTime value = entry.getValue();
                if (remoteRecords.containsKey(key) && remoteRecords.get(key).getType().equals(value.getType())) { // 操作的key相通，操作类型也相同
                    localRecords.remove(key);
                    remoteRecords.remove(key);
                }
            }
        }

        // 打印localRecords和remoteRecords
        System.out.println("localRecords:");        
        for (Map.Entry<String, TypeWithTime> entry : localRecords.entrySet()) {
            System.out.println("key:" + entry.getKey() + ",value:" + entry.getValue());
        }
        System.out.println("remoteRecords:");
        for (Map.Entry<String, TypeWithTime> entry : remoteRecords.entrySet()) {
            System.out.println("key:" + entry.getKey() + ",value:" + entry.getValue());
        }

        Long remoteOldestTime = remoteRecords.values().stream().map(TypeWithTime::getTime).min(Long::compareTo)
                .orElse(Long.MAX_VALUE);
        Long localOldestTime = localRecords.values().stream().map(TypeWithTime::getTime).min(Long::compareTo)
                .orElse(Long.MAX_VALUE);
        Long curTime = System.currentTimeMillis();
        if (remoteOldestTime < curTime - Long.parseLong(timeFram)
                || localOldestTime < curTime - Long.parseLong(timeFram)) {
            isConcistByRecord.put(dbAndTable, false);
        } else {
            isConcistByRecord.put(dbAndTable, true);
        }
    }

    public void printAllConsistByRecord() {
        if (!isServer)
            LOG.debug("isConcistByRecord:");
        for (Map.Entry<String, Boolean> entry : isConcistByRecord.entrySet()) {
            LOG.info("dbAndTable:" + entry.getKey() + ",isConsistByRecord:" + entry.getValue());
        }
    }

    public void printAllConsistByMerkleHash() {
        if (!isServer) {
            LOG.debug("isConcistByMerkleHash:");
            for (Map.Entry<String, Boolean> entry : isConcistByMerkleHash.entrySet()) {
                LOG.info("dbAndTable:" + entry.getKey() + ",isConsistByMerkleHash:" + entry.getValue());
            }
        }

    }

    /*
     * 如果当前是client端，接受到remoteRecord之后，则将remoteRecord对应的localRecord删除
     */
    public void matchRecord(String dbAndTable, String key, TypeWithTime remoteRecord) {
        ConcurrentHashMap<String, TypeWithTime> localRecords = localBinRecords.get(dbAndTable);
        if (localRecords != null) {
            TypeWithTime localTypeWithTime = localRecords.get(key);
            if (localTypeWithTime != null && localTypeWithTime.equals(remoteRecord)) {
                localRecords.remove(key);
            }
        }
    }

    // 执行查询来获取所有的表名
    public List<String> getAllTableNames() {
        String sql = "SHOW TABLES";
        return jdbcTemplate.queryForList(sql, String.class);
    }

    // 将所有BP树刷入磁盘
    public void flushAllBtree() {
        for (Map.Entry<String, BTree> entry : localBTrees.entrySet()) {
            String dbAndTable = entry.getKey();
            BTree btree = entry.getValue();
            try {
                btree.flush();
            } catch (BTreeException e) {
                e.printStackTrace();
            }
        }
    }

    // 根据数据库的表初始化BP树
    public void initBtree() {
        List<String> tables = getAllTableNames();
        String dbName = getDatabaseNameFromUrl(url);
        for (String table : tables) {
            String dbAndTable = dbName + "__" + table;
            try {
                BTree bTree = getBTree(dbAndTable);
                // 先查找该table的key和update_time_on_chain字段
                // `key` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
                // `update_time_on_chain` timestamp(3) NULL DEFAULT NULL ON UPDATE
                // CURRENT_TIMESTAMP(3),
                // String sql = "SELECT key, update_time_on_chain FROM " + table;
                // List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql);
                // for (Map<String, Object> row : rows) {
                // String key = (String) row.get("key");
                // String update_time_on_chain = row.get("update_time_on_chain").toString();
                // long time = Utils.convertStringToLong(update_time_on_chain);
                // addRecordToInsertRecord(dbAndTable, time, key);
                // }

            } catch (BTreeException e) {
                e.printStackTrace();
            }
        }
    }

    public String getDatabaseNameFromUrl(String url) {
        // 移除参数部分（如果有的话）
        String urlWithoutParams = url.split("\\?")[0];
        // 提取最后一个斜杠后的部分作为数据库名
        return urlWithoutParams.substring(urlWithoutParams.lastIndexOf("/") + 1);
    }

    public Object getCompareResult(String channelType, String channelName) {
        String dbName = this.getDatabaseNameFromUrl(url);
        Boolean resultByMerkleHash = isConcistByMerkleHash.get(dbName + "__" + channelType + "_" + channelName);
        Boolean resultByRecord = isConcistByRecord.get(dbName + "__" + channelType + "_" + channelName);
        if (resultByMerkleHash == null || resultByRecord == null)
            return "指定的通道还未存在一致性对比结果";
        if (strictMode)
            return resultByMerkleHash && resultByRecord;
        else
            return resultByMerkleHash || resultByRecord;

    }

}
