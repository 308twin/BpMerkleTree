package btree4j.schedule;

import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.PostConstruct;

import org.apache.rocketmq.client.apis.ClientException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import btree4j.entity.ConcurrentLimitedSortedStore;
import btree4j.service.CompareService;
import btree4j.service.MqService;

import java.io.IOException;
import java.util.*;

@Component
public class BPMerkleTreeSchedule {
    @org.springframework.beans.factory.annotation.Value("${my.custom.config.isServer}")
    private boolean isServer;

    @org.springframework.beans.factory.annotation.Value("${my.custom.config.usingBPTree}")
    private boolean usingBPTree;

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(BPMerkleTreeSchedule.class);

    private ConcurrentHashMap<String,ConcurrentLimitedSortedStore> aboutToInsertRecord;
    private CompareService compareService;
    private MqService   mqService;

    public BPMerkleTreeSchedule(ConcurrentHashMap<String,ConcurrentLimitedSortedStore> aboutToInsertRecord, 
    CompareService compareService,
    MqService MqService) {
        this.aboutToInsertRecord = aboutToInsertRecord;
        this.compareService = compareService;
        this.mqService = MqService;
    }

    @Scheduled(fixedRate = 1000)
    public void buildTree(){
        if(!usingBPTree)
            return;

        Long curTimestamp = System.currentTimeMillis();
        // 遍历aboutToInsertRecord，将其中的key插入到btree中
        for (Map.Entry<String,ConcurrentLimitedSortedStore> entry : aboutToInsertRecord.entrySet()){
            String dbAndTable = entry.getKey();
            ConcurrentLimitedSortedStore value = entry.getValue();
            for (Map.Entry<String,Long> entry1 : value.entrySet()){
                try {
                    LOG.debug("insert key to btree:"+entry1.getKey());
                    compareService.insertKeyToBtree(dbAndTable,entry1.getKey(),entry1.getValue());
                    value.remove(entry1.getKey());
                    String newestHash = compareService.getBTreeRootMerkleHash(dbAndTable);
                    compareService.insertHashToLocalHashs(dbAndTable, newestHash);
                    if(entry1.getValue() > curTimestamp - 5000){
                        break;
                    }
                    //System.out.println("insert key to btree success,newest btree root hash:"+newestHash);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    

    //将BP树刷入磁盘，60s一次
    @Scheduled(fixedRate = 60000)
    public void flushBtree(){
        compareService.flushAllBtree();
    }


    @PostConstruct
    public void initBtree(){
        compareService.initBtree();
    }

    @Scheduled(fixedRate = 5000)
    public void printLocalBinRecords(){
        if(isServer)
            return;
        mqService.printLocalBinRecordsWhereTimeRangeBiggerThan5s();
    }

    //每10s打印一次record是否一致
    @Scheduled(fixedRate = 10000)
    public void printIsConsistByRecord(){
        if(!isServer){
        compareService.matchAllRecords();
        compareService.printAllConsistByRecord();
        }
        
    }

    //每10s打印一次merkleHash是否一致    
    @Scheduled(fixedRate = 10000)
    public void printIsConsistByMerkleHash(){
        if(!isServer){
        compareService.matchAllHashs();
        compareService.printAllConsistByMerkleHash();
        }
    }

    @Scheduled(fixedRate = 1000)
    public void sendLocalRecordsToRemote() throws ClientException, IOException{
        mqService.sendLocalRecordsToRemote();
    }


    @Scheduled(fixedRate = 1000)
    public void sendLocalHashsToRemote() throws ClientException, IOException{
        mqService.sendLocalHashsToRemote();
    }

   
}
