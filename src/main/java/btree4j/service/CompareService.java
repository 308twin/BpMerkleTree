package btree4j.service;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Service;

import btree4j.BTree;
import btree4j.BTreeException;
import btree4j.Value;
import btree4j.entity.LimitedSizeConcurrentSkipListMapDescending;
import btree4j.entity.MerkleHashEntity;
import btree4j.entity.TypeWithTime;
import btree4j.utils.io.FileUtils;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

@Service
@ConfigurationProperties(prefix = "my.custom.config")
public class CompareService {
    @org.springframework.beans.factory.annotation.Value("${my.custom.config.timeFram}")
    private String timeFram;

    private boolean isServer;
    private ConcurrentHashMap<String, Map> localHashs;
    private ConcurrentHashMap<String, Map> remoteHashs;
    private ConcurrentHashMap<String, Boolean> isConcistByMerkleHash;
    private ConcurrentHashMap<String, BTree> localBTrees;
    private ConcurrentHashMap<String, Map<Long, String>> aboutToInsertRecord;
    private ConcurrentHashMap<String, ConcurrentHashMap<String, TypeWithTime>> remoteBinReocrds;
    private ConcurrentHashMap<String, ConcurrentHashMap<String, TypeWithTime>> localBinRecords;
    private ConcurrentHashMap<String, Boolean> isConcistByRecord;

    @Autowired
    public CompareService(@Qualifier("localHashs") ConcurrentHashMap<String, Map> localHashs,
            ConcurrentHashMap<String, Map> remoteHashs,
            ConcurrentHashMap<String, BTree> localBTrees,
            ConcurrentHashMap<String, Map<Long, String>> aboutToInsertRecord,
            ConcurrentHashMap<String, Boolean> isConcistByMerkleHash,
            @Qualifier("remoteBinReocrds") ConcurrentHashMap<String, ConcurrentHashMap<String, TypeWithTime>> remoteBinReocrds,
            @Qualifier("localBinRecords") ConcurrentHashMap<String, ConcurrentHashMap<String, TypeWithTime>> localBinRecords,
            ConcurrentHashMap<String, Boolean> isConcistByRecord) {
        this.localHashs = localHashs;
        this.remoteHashs = remoteHashs;
        this.localBTrees = localBTrees;
        this.aboutToInsertRecord = aboutToInsertRecord;
        this.isConcistByMerkleHash = isConcistByMerkleHash;
        this.remoteBinReocrds = remoteBinReocrds;
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

    public void insertHashToLocalHashs(String dbAndTable, String hash) {
        Map<Long, String> tableHashHistorys = localHashs.get(dbAndTable);
        if (tableHashHistorys == null) {
            tableHashHistorys = new LimitedSizeConcurrentSkipListMapDescending(200000);
            localHashs.put(dbAndTable, tableHashHistorys);
        }
        // current time to long
        long time = System.currentTimeMillis();
        tableHashHistorys.put(time, hash);
    }

    public void insertKeyToBtree(String dbAndTable, String value, long time) throws BTreeException {
        BTree btree = getBTree(dbAndTable);
        Value k = new Value(value);
        btree.addValue(k, time);
    }

    public void removeKeyFromBtree(String dbAndTable, String value, long time) throws BTreeException {
        BTree btree = getBTree(dbAndTable);
        Value k = new Value(value);
        btree.removeValue(k, time);
    }

    public String getBTreeRootMerkleHash(String dbAndTable) throws BTreeException {
        BTree btree = getBTree(dbAndTable);
        return btree.getRootMerkleHash();
    }

    public void addRecordToInsertRecord(String dbAndTable, long time, String value) {
        Map<Long, String> valueMap = aboutToInsertRecord.get(dbAndTable); // 获取dbAndTable对应的valueMap
        if (valueMap == null) {
            valueMap = new ConcurrentSkipListMap<>(new Comparator<Long>() {
                @Override
                public int compare(Long o1, Long o2) {
                    return Long.compare(o1, o2); // 使用 Long.compare 进行比较
                }
            });
            aboutToInsertRecord.put(dbAndTable, valueMap);
        }
        valueMap.put(time, value);
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
        localHashs.remove(dbAndTable);
        remoteHashs.remove(dbAndTable);
    }

    /*
     * 如果remoteBinRecords中的记录和localBinRecords中的记录为空，则更新isConcistByRecord对应的值为true
     * 如果remoteBinRecords中的最旧的记录和localBinRecords中最旧的记录有时间(超过当前时间-timeFram)的记录，
     * 则更新isConcistByRecord对应的值为false
     */
    public void isConcistByRecord(String dbAndTable) {
        ConcurrentHashMap<String, TypeWithTime> localRecords = localBinRecords.get(dbAndTable);
        ConcurrentHashMap<String, TypeWithTime> remoteRecords = remoteBinReocrds.get(dbAndTable);

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
}
