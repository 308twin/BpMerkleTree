package btree4j.cache;

import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import btree4j.BTree;
import btree4j.entity.MerkleHashEntity;
import btree4j.entity.TypeWithTime;

import java.util.concurrent.ConcurrentHashMap;
import org.springframework.context.annotation.Bean;
import java.time.format.DateTimeFormatter;
import org.springframework.jdbc.core.JdbcTemplate;
import java.util.concurrent.ConcurrentSkipListMap;
import javax.sql.DataSource;
import java.util.*;

@Configuration
public class MerkleHashCache {

    // key : dbname__tablename
    // value : merkleHash
    // 存储本地生成的历史merkleHash
    @Bean("localHashs")
    public ConcurrentHashMap<String,Map> localHashs() {
        return new ConcurrentHashMap<>();
    }

    // key : dbname__tablename
    // value : merkleHash
    // 存储远程生成的历史merkleHash
    @Bean("remoteHashs")
    public ConcurrentHashMap<String,Map> remoteHashs() {
        return new ConcurrentHashMap<>();
    }

    // key : dbname__tablename
    // value : Boolean
    // 存储本地和远程merkleHash是否一致
    @Bean("isConcistByMerkleHash")
    public ConcurrentHashMap<String,Boolean> isConcistByMerkleHash() {
        return new ConcurrentHashMap<>();
    }

    // key : dbname__tablename
    // value : Boolean
    // 存储本地和远程record是否一致
    @Bean("isConcistByRecord")
    public ConcurrentHashMap<String,Boolean> isConcistByRecord() {
        return new ConcurrentHashMap<>();
    }
    
    // key : dbname__tablename
    // value : BTree
    // 存储本地的BTree
    @Bean("localBTrees")
    public ConcurrentHashMap<String,BTree> localBTrees() {
        return new ConcurrentHashMap<>();
    }

    //key : dbname__tablename
    //value : key ,typeWithTime
    //存储远程binlog记录
    @Bean("remoteBinRecords")
    public ConcurrentHashMap<String,ConcurrentHashMap<String,TypeWithTime>> remoteBinRecords() {
        return new ConcurrentHashMap<>();
    }

    //key : dbname__tablename
    //value : key ,typeWithTime
    //存储本地binlog记录
    @Bean("localBinRecords")
    public ConcurrentHashMap<String,ConcurrentHashMap<String,TypeWithTime>> localBinRecords() {
        return new ConcurrentHashMap<>();
    }

    //schedule来处理
    @Bean("aboutToInsertRecord")
    public ConcurrentHashMap<String,Map<Long,String>> aboutToInsertRecord(){
        return new ConcurrentHashMap<>();
    }

    @Bean
    public JdbcTemplate jdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

}
