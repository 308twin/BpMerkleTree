package btree4j.schedule;

import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.PostConstruct;

import org.apache.rocketmq.client.apis.ClientException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import btree4j.service.CompareService;
import btree4j.service.MqService;

import java.io.IOException;
import java.util.*;

@Component
public class BPMerkleTreeSchedule {
    private ConcurrentHashMap<String,Map<Long,String>> aboutToInsertRecord;
    private CompareService compareService;
    private MqService   mqService;

    public BPMerkleTreeSchedule(ConcurrentHashMap<String,Map<Long,String>> aboutToInsertRecord, 
    CompareService compareService,
    MqService MqService) {
        this.aboutToInsertRecord = aboutToInsertRecord;
        this.compareService = compareService;
        this.mqService = MqService;
    }

    @Scheduled(fixedRate = 10000)
    public void buildTree(){
        for (Map.Entry<String,Map<Long,String>> entry : aboutToInsertRecord.entrySet()){
            String dbAndTable = entry.getKey();
            Map<Long,String> value = entry.getValue();
            for (Map.Entry<Long,String> entry1 : value.entrySet()){
                try {
                    compareService.insertKeyToBtree(dbAndTable,entry1.getValue(),entry1.getKey());
                    value.remove(entry1.getKey());
                    String newestHash = compareService.getBTreeRootMerkleHash(dbAndTable);
                    compareService.insertHashToLocalHashs(dbAndTable, newestHash);
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

    @Scheduled(fixedRate = 10000)
    public void printLocalBinRecords(){
        mqService.printLocalBinRecordsWhereTimeRangeBiggerThan5s();
    }

    //每10s打印一次record是否一致
    @Scheduled(fixedRate = 10000)
    public void printIsConsistByRecord(){
        compareService.printAllConsistByRecord();
    }

    //每10s打印一次merkleHash是否一致
    @Scheduled(fixedRate = 10000)
    public void printIsConsistByMerkleHash(){
        compareService.printAllConsistByMerkleHash();
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
