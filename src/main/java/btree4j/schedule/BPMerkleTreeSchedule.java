package btree4j.schedule;

import java.util.concurrent.ConcurrentHashMap;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import btree4j.service.CompareService;

import java.util.*;

@Component
public class BPMerkleTreeSchedule {
    private ConcurrentHashMap<String,Map<Long,String>> aboutToInsertRecord;
    private CompareService compareService;

    public BPMerkleTreeSchedule(ConcurrentHashMap<String,Map<Long,String>> aboutToInsertRecord, CompareService compareService) {
        this.aboutToInsertRecord = aboutToInsertRecord;
        this.compareService = compareService;
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
                    System.out.println("insert key to btree success,newest btree root hash:"+newestHash);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
