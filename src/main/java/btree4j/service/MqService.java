package btree4j.service;

import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import btree4j.entity.TypeWithTime;
import java.util.*;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientConfigurationBuilder;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

@Service
public class MqService {

    private static final Log LOG = LogFactory.getLog(MqService.class);

    @org.springframework.beans.factory.annotation.Value("${my.custom.config.isServer}")
    private boolean isServer;

    @org.springframework.beans.factory.annotation.Value("${spring.rocketmq.proxy.server-address}")
    private String proxyServerAddress;

    private ConcurrentHashMap<String, ConcurrentHashMap<String, TypeWithTime>> remoteBinRecords;
    private ConcurrentHashMap<String, ConcurrentHashMap<String, TypeWithTime>> localBinRecords;
    private ConcurrentHashMap<String, Map> localHashs;
    private ConcurrentHashMap<String, Map> remoteHashs;
    private CompareService compareService;

    public MqService(ConcurrentHashMap<String, ConcurrentHashMap<String, TypeWithTime>> remoteBinRecords,
            ConcurrentHashMap<String, ConcurrentHashMap<String, TypeWithTime>> localBinRecords,
            ConcurrentHashMap<String, Map> localHashs, ConcurrentHashMap<String, Map> remoteHashs,
            CompareService compareService) {
        this.remoteBinRecords = remoteBinRecords;
        this.localBinRecords = localBinRecords;
        this.localHashs = localHashs;
        this.remoteHashs = remoteHashs;
        this.compareService = compareService;
    }

    public void createMqTopic() throws MQClientException {
        if (isServer) {
            List<String> tables = compareService.getAllTableNames();
            DefaultMQAdminExt mqAdminExt = new DefaultMQAdminExt();
            mqAdminExt.setNamesrvAddr(proxyServerAddress);
            mqAdminExt.start(); // 启动 admin 实例
            // create topic if not exist
            for (String table : tables) {
                try {
                    String topicName = table;
                    int queueNum = 8;
                    TopicConfig topicConfig = new TopicConfig();
                    topicConfig.setTopicName(topicName);
                    topicConfig.setWriteQueueNums(queueNum);
                    topicConfig.setReadQueueNums(queueNum);
                    mqAdminExt.createAndUpdateTopicConfig(proxyServerAddress, topicConfig);

                } catch (Exception e) {
                    LOG.error("create topic failed", e);
                }
            }

        }
    }

}
