package btree4j.service;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Service;
import org.springframework.jdbc.core.JdbcTemplate;
import btree4j.BTree;
import btree4j.BTreeException;
import btree4j.Value;
import btree4j.entity.LimitedSizeConcurrentSkipListMapDescending;
import btree4j.entity.MerkleHashEntity;
import btree4j.entity.TypeWithTime;
import btree4j.utils.Utils;
import btree4j.utils.io.FileUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Cache;
import java.util.concurrent.TimeUnit;
@Service
@ConfigurationProperties(prefix = "my.custom.config")
public class CompareService {
    @org.springframework.beans.factory.annotation.Value("${my.custom.config.timeFram}")
    private String timeFram;

    @org.springframework.beans.factory.annotation.Value("${my.custom.config.localHashMapMaxSize}")
    private int localHashMapMaxSize;

    @org.springframework.beans.factory.annotation.Value("${spring.datasource.url}")
    private String url;


    private boolean isServer;
    private ConcurrentHashMap<String, Map> localHashs;
    private ConcurrentHashMap<String, Map> remoteHashs;
    private ConcurrentHashMap<String, Map> aboutToSendHashs;
    private ConcurrentHashMap<String, Boolean> isConcistByMerkleHash;
    private ConcurrentHashMap<String, BTree> localBTrees;
    private ConcurrentHashMap<String, Map<Long, String>> aboutToInsertRecord;
    private ConcurrentHashMap<String, ConcurrentHashMap<String, TypeWithTime>> remoteBinRecords;
    private ConcurrentHashMap<String, ConcurrentHashMap<String, TypeWithTime>> localBinRecords;
    private ConcurrentHashMap<String, Boolean> isConcistByRecord;
     

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    public CompareService(@Qualifier("localHashs") ConcurrentHashMap<String, Map> localHashs,
            ConcurrentHashMap<String, Map> remoteHashs,
            ConcurrentHashMap<String, Map> aboutToSendHashs,
            ConcurrentHashMap<String, BTree> localBTrees,
            ConcurrentHashMap<String, Map<Long, String>> aboutToInsertRecord,
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
        Map<Long, String> tableHashHistorys = localHashs.computeIfAbsent(dbAndTable, 
        k -> new LimitedSizeConcurrentSkipListMapDescending(localHashMapMaxSize));

        Map<Long, String> aboutToSendHistorys = aboutToSendHashs.computeIfAbsent(dbAndTable,
        k -> new LimitedSizeConcurrentSkipListMapDescending(localHashMapMaxSize));
       

        // Map<Long, String> tableHashHistorys = localHashs.get(dbAndTable);
        // if (tableHashHistorys == null) {
        //     tableHashHistorys = new LimitedSizeConcurrentSkipListMapDescending(localHashMapMaxSize);
        //     localHashs.put(dbAndTable, tableHashHistorys);
        // }
        // current time to long
        long time = System.currentTimeMillis();
        tableHashHistorys.put(time, hash);
        aboutToSendHistorys.put(time, hash);
    }

    public void insertKeyToBtree(String dbAndTable, String value, long time) throws BTreeException {
        BTree btree = getBTree(dbAndTable);
        Value k = new Value(value);
        btree.addValue(k, time);
        System.out.println("Success insert key:" + value + ",newest root hash is : " + btree.getRootMerkleHash());
    }

    public void removeKeyFromBtree(String dbAndTable, String value, long time) throws BTreeException {
        BTree btree = getBTree(dbAndTable);
        Value k = new Value(value);
        btree.removeValue(k, time);
        System.out.println("Success remove key:" + value + ",newest root hash is : " + btree.getRootMerkleHash());
    }

    public String getBTreeRootMerkleHash(String dbAndTable) throws BTreeException {
        BTree btree = getBTree(dbAndTable);
        return btree.getRootMerkleHash();
    }

    // 将记录插入到待插入列表，使用concurrentSkipListMap存储,排序方式是按照时间戳排序
    public void addRecordToInsertRecord(String dbAndTable, long time, String value) {
        Map<Long, String> valueMap = aboutToInsertRecord.computeIfAbsent(
            dbAndTable,
            k -> new ConcurrentSkipListMap<>(Comparator.comparingLong(Long::longValue))
        );
        // if (valueMap == null) {
        //     valueMap = new ConcurrentSkipListMap<>(new Comparator<Long>() {
        //         @Override
        //         public int compare(Long o1, Long o2) {
        //             return Long.compare(o1, o2); // 使用 Long.compare 进行比较，时间戳小的在前
        //         }
        //     });
        //     aboutToInsertRecord.put(dbAndTable, valueMap);
        // }
        valueMap.put(time, value);
    }

    public void addToRemoteHashs(String dbAndTable, long time, String hash) {   
        @SuppressWarnings("unchecked")
        Map<Long,String> remoteHashsMap = remoteHashs.computeIfAbsent(
            dbAndTable,
            k -> new ConcurrentSkipListMap<>(Comparator.comparingLong(Long::longValue))
        );
        remoteHashsMap.put(time, hash);
    }

    // 通过remoteHashs中的hash和localHashs中的hash进行比较，如果不一致则返回false
    // 判断的范围是根据localHashs的全部内容
    // 具体比较规则：获取dbAndTable对应的localHashs和remoteHashs，遍历localHashs，从更大的时间开始往前比较，如果存在某个hash一致，则更新isConcistByMerkleHash对应的值为true
    // 如果比较到timeFram之前的时间都没有一致的hash，则更新isConcistByMerkleHash对应的值为false
    // 如果remoteHashs中的hash和localHashs中的hash都为空，则不修改isConcistByMerkleHash对应的值
    // 最后，把localHashs和remoteHashs中的数据清空
    public void isConcistByMerkleHash(String dbAndTable) {
        Map<Long, String> localHashsMap = localHashs.get(dbAndTable);
        Map<Long, String> remoteHashsMap = remoteHashs.get(dbAndTable);
        if (localHashsMap == null && remoteHashsMap == null) {
            return;
        }
        if (localHashsMap == null || remoteHashsMap == null) {
            isConcistByMerkleHash.put(dbAndTable, false);
            return;
        }
        boolean isConcist = false;
        for (Map.Entry<Long, String> entry : localHashsMap.entrySet()) {
            Long time = entry.getKey();
            String hash = entry.getValue();
            if (remoteHashsMap.containsKey(time) && remoteHashsMap.get(time).equals(hash)) {
                isConcist = true;
                break;
            }
            if (time < Long.parseLong(timeFram)) {
                break;
            }
        }
        isConcistByMerkleHash.put(dbAndTable, isConcist);
        // localHashs.remove(dbAndTable);
        // remoteHashs.remove(dbAndTable);
    }

    public void addToLocalBinRecords(String dbName, String tableName, String key, TypeWithTime typeWithTime) {
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

    
    public void matchAllRecords(){
        List<String> dbAndTables = new ArrayList<>();
        dbAndTables.addAll(localBinRecords.keySet());
        for (String dbAndTable : dbAndTables) {
            isConcistByRecord(dbAndTable);
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

        if (localRecords == null && remoteRecords == null) {
            isConcistByRecord.put(dbAndTable, true);
            return;
        }
        // 遍历localRecords,如果localRecord中的TypeWithTime的值存在于remoteRecords中，则删除localRecord中的TypeWithTime，以及remoteRecords中的TypeWithTime
        if (localRecords != null && remoteRecords != null) {
            for (Map.Entry<String, TypeWithTime> entry : localRecords.entrySet()) {
                String key = entry.getKey();
                TypeWithTime value = entry.getValue();
                if (remoteRecords.containsKey(key) && remoteRecords.get(key).equals(value)) {
                    localRecords.remove(key);
                    remoteRecords.remove(key);
                }
            }
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

    public void printAllConsistByRecord(){
        for (Map.Entry<String, Boolean> entry : isConcistByRecord.entrySet()){
            System.out.println("dbAndTable:"+entry.getKey()+",isConsistByRecord:"+entry.getValue());
        }
    }

    public void printAllConsistByMerkleHash(){
        for (Map.Entry<String, Boolean> entry : isConcistByMerkleHash.entrySet()){
            System.out.println("dbAndTable:"+entry.getKey()+",isConsistByMerkleHash:"+entry.getValue());
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

    //将所有BP树刷入磁盘
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

    //根据数据库的表初始化BP树
    public void initBtree() {
        List<String> tables = getAllTableNames();
        String dbName = getDatabaseNameFromUrl(url);
        for (String table : tables) {
            String dbAndTable =  dbName + "__" + table;
            try {
                BTree bTree = getBTree(dbAndTable);
                //先查找该table的key和update_time_on_chain字段
                //`key` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
                //`update_time_on_chain` timestamp(3) NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP(3),
                // String sql = "SELECT key, update_time_on_chain FROM " + table;
                // List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql);
                // for (Map<String, Object> row : rows) {
                //     String key = (String) row.get("key");
                //     String update_time_on_chain = row.get("update_time_on_chain").toString();
                //     long time = Utils.convertStringToLong(update_time_on_chain);
                //     addRecordToInsertRecord(dbAndTable, time, key);
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

    
    

}
