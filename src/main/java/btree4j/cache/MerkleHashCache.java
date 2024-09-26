package btree4j.cache;

import org.springframework.context.annotation.Configuration;

import btree4j.BTree;
import btree4j.entity.MerkleHashEntity;
import btree4j.entity.TypeWithTime;

import java.util.concurrent.ConcurrentHashMap;
import org.springframework.context.annotation.Bean;
import java.time.format.DateTimeFormatter;

import java.util.*;

@Configuration
public class MerkleHashCache {

    @Bean("localHashs")
    public ConcurrentHashMap<String,Map> localHashs() {
        return new ConcurrentHashMap<>();
    }

    @Bean("remoteHashs")
    public ConcurrentHashMap<String,Map> remoteHashs() {
        return new ConcurrentHashMap<>();
    }

    @Bean("isConcistByMerkleHash")
    public ConcurrentHashMap<String,Boolean> isConcistByMerkleHash() {
        return new ConcurrentHashMap<>();
    }

    @Bean("isConcistByRecord")
    public ConcurrentHashMap<String,Boolean> isConcistByRecord() {
        return new ConcurrentHashMap<>();
    }
    
    // key : dbname__tablename
    // value : BTree
    @Bean("localBTrees")
    public ConcurrentHashMap<String,BTree> localBTrees() {
        return new ConcurrentHashMap<>();
    }



    //key : dbname__tablename
    //value : key ,typeWithTime

    @Bean("remoteBinRecords")
    public ConcurrentHashMap<String,ConcurrentHashMap<String,TypeWithTime>> remoteBinRecords() {
        return new ConcurrentHashMap<>();
    }

    @Bean("localBinRecords")
    public ConcurrentHashMap<String,ConcurrentHashMap<String,TypeWithTime>> localBinRecords() {
        return new ConcurrentHashMap<>();
    }

    //schedule来处理
    @Bean("aboutToInsertRecord")
    public ConcurrentHashMap<String,Map<Long,String>> aboutToInsertRecord(){
        return new ConcurrentHashMap<>();
    }

}
