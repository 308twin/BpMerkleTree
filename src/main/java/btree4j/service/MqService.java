package btree4j.service;

import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import btree4j.entity.BinRecord;
import btree4j.entity.HashWithTimestamp;
import btree4j.entity.TypeWithTime;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.*;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientConfigurationBuilder;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.shaded.com.google.protobuf.Timestamp;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

@Service
public class MqService {

    private static final Log LOG = LogFactory.getLog(MqService.class);

    @org.springframework.beans.factory.annotation.Value("${my.custom.config.isServer}")
    private boolean isServer;

    @org.springframework.beans.factory.annotation.Value("${spring.rocketmq.proxy.server-address}")
    private String proxyServerAddress;

    @org.springframework.beans.factory.annotation.Value("${spring.rocketmq.topic.record}")
    private String recordTopic;

    @org.springframework.beans.factory.annotation.Value("${spring.rocketmq.topic.hash}")
    private String hashTopic;

    @org.springframework.beans.factory.annotation.Value("${spring.datasource.url}")
    private String url;

    // private String dbName;

    private Producer producer; // 将 Producer 保持为全局的
    private ClientServiceProvider provider;
    private ClientConfiguration clientConfiguration;
    private PushConsumer recordPushConsumer;
    private PushConsumer hashPushConsumer;
    private static final ThreadLocal<Kryo> kryoThreadLocal = ThreadLocal.withInitial(Kryo::new);

    private ConcurrentHashMap<String, ConcurrentHashMap<String, TypeWithTime>> remoteBinRecords;
    private ConcurrentHashMap<String, ConcurrentHashMap<String, TypeWithTime>> localBinRecords;
    private ConcurrentHashMap<String, Map> localHashs;
    private ConcurrentHashMap<String, Map> aboutToSendHashs;
    private ConcurrentHashMap<String, Map> remoteHashs;
    private CompareService compareService;

    public MqService(ConcurrentHashMap<String, ConcurrentHashMap<String, TypeWithTime>> remoteBinRecords,
            ConcurrentHashMap<String, ConcurrentHashMap<String, TypeWithTime>> localBinRecords,
            ConcurrentHashMap<String, Map> localHashs, ConcurrentHashMap<String, Map> remoteHashs,
            ConcurrentHashMap<String, Map> aboutToSendHashs,
            CompareService compareService) {
        this.remoteBinRecords = remoteBinRecords;
        this.localBinRecords = localBinRecords;
        this.localHashs = localHashs;
        this.remoteHashs = remoteHashs;
        this.aboutToSendHashs = aboutToSendHashs;
        this.compareService = compareService;
    }

    @PostConstruct
    public void init() {
        if (isServer) {
            System.out.println("Running as WebSocket Server for Record");
        } else {
            
            System.out.println("Running as WebSocket Client for Record");
        }
        // dbName = compareService.getDatabaseNameFromUrl(url);
    }

    @PostConstruct
    public void initProducer() throws ClientException {
        if (isServer) {
            provider = ClientServiceProvider.loadService();
            clientConfiguration = ClientConfiguration.newBuilder()
                    .setEndpoints(proxyServerAddress)
                    .build();
            ClientConfigurationBuilder builder = ClientConfiguration.newBuilder().setEndpoints(proxyServerAddress);
            ClientConfiguration configuration = builder.build();
            producer = provider.newProducerBuilder()
                    .setClientConfiguration(configuration)
                    .build();
        }

    }

    //@PostConstruct
    public void initRecordConsumer() throws ClientException {
        if (!isServer) {
            provider = ClientServiceProvider.loadService();
            clientConfiguration = ClientConfiguration.newBuilder()
            .setEndpoints(proxyServerAddress)
            .build();
            // 初始化 PushConsumer
            String topic = recordTopic;
            String dbName = compareService.getDatabaseNameFromUrl(url);
            List<String> tags = compareService.getAllTableNames();
            for (int index = 0; index < tags.size(); index++) {
                tags.set(index, dbName + "__" + tags.get(index));
            }
            String tagString = String.join("||", tags);
            FilterExpression filterExpression = new FilterExpression(tagString, FilterExpressionType.TAG);

            recordPushConsumer = provider.newPushConsumerBuilder()
                    .setClientConfiguration(clientConfiguration)
                    .setConsumerGroup("record_consumer") // 设置 Consumer Group
                    .setSubscriptionExpressions(Collections.singletonMap("hash", filterExpression))
                    .setMessageListener(messageView -> {
                        // LOG.info("Consume message successfully, messageId="+
                        // messageView.getMessageId());
                        processRecordMessage(messageView);
                        return ConsumeResult.SUCCESS;
                    })
                    .build();
        }

    }

    @PostConstruct
    public void initHashConsumer() throws ClientException {
        if (!isServer) {
            provider = ClientServiceProvider.loadService();
            clientConfiguration = ClientConfiguration.newBuilder()
            .setEndpoints(proxyServerAddress)
            .build();

            // 初始化 PushConsumer
            String topic = hashTopic;
            String dbName = compareService.getDatabaseNameFromUrl(url);
            List<String> tags = compareService.getAllTableNames();
            for (int index = 0; index < tags.size(); index++) {
                tags.set(index, dbName + "__" + tags.get(index));
            }
            String tagString = String.join("||", tags);
            FilterExpression filterExpression = new FilterExpression(tagString, FilterExpressionType.TAG);

            hashPushConsumer = provider.newPushConsumerBuilder()
                    .setClientConfiguration(clientConfiguration)
                    .setConsumerGroup("hash_consumer") // 设置 Consumer Group
                    .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
                    .setMessageListener(messageView -> {
                        processHashMessage(messageView);
                        return ConsumeResult.SUCCESS;
                    })
                    .build();
        }
    }

    public void sendLocalRecordsToRemote() throws ClientException, IOException {
        if (isServer) {
            // 遍历localBinRecords，构建消息，发送到proxyServer,发送后删除
            for (Map.Entry<String, ConcurrentHashMap<String, TypeWithTime>> entry : localBinRecords.entrySet()) {
                String dbAndTable = entry.getKey();
                ConcurrentHashMap<String, TypeWithTime> records = entry.getValue();
                Kryo kryo = kryoThreadLocal.get();
                ByteArrayOutputStream byteOut = new ByteArrayOutputStream(); // 重用字节输出流
                Output output = new Output(byteOut); // 重用 Kryo 的 Output 对象
                for (Map.Entry<String, TypeWithTime> record : records.entrySet()) {
                    String key = record.getKey();
                    TypeWithTime typeWithTime = record.getValue();

                    BinRecord binRecord = new BinRecord(key, typeWithTime.getTime(), typeWithTime.getType());
                    byteOut.reset();
                    kryo.writeObject(output, binRecord);
                    output.flush();

                    byte[] serializedBytes = byteOut.toByteArray(); // 获取序列化后的字节数组

                    Message message = provider.newMessageBuilder()
                            .setTopic(recordTopic)
                            .setTag(dbAndTable)
                            .setBody(serializedBytes)
                            .build();

                    try {
                        // 发送消息，需要关注发送结果，并捕获失败等异常。
                        SendReceipt sendReceipt = producer.send(message);
                        LOG.info("Send message successfully, messageId={}" + sendReceipt.getMessageId() +
                                "tag=" + dbAndTable);
                        // 发送成功后删除
                        records.remove(key);
                    } catch (ClientException e) {
                        LOG.error("Failed to send message", e);
                    }
                }
            }
        }
    }

    // 发送本地hash记录到远程
    public void sendLocalHashsToRemote() {
        if (isServer) {
            // 遍历localHashRecords，构建消息，发送到proxyServer,发送后删除
            for (Map.Entry<String, Map> entry : aboutToSendHashs.entrySet()) {
                String dbAndTable = entry.getKey();
                Map<Long, String> records = entry.getValue();
                Kryo kryo = kryoThreadLocal.get();
                ByteArrayOutputStream byteOut = new ByteArrayOutputStream(); // 重用字节输出流
                Output output = new Output(byteOut); // 重用 Kryo 的 Output 对象
                for (Map.Entry<Long, String> record : records.entrySet()) {
                    Long key = record.getKey();
                    String value = record.getValue();
                    byteOut.reset();

                    // 写入record
                    HashWithTimestamp hashWithTimestamp = new HashWithTimestamp(value, key);
                    kryo.writeObject(output, hashWithTimestamp);
                    output.flush();

                    byte[] serializedBytes = byteOut.toByteArray(); // 获取序列化后的字节数组
                   
                    Message message = provider.newMessageBuilder()
                            .setTopic(hashTopic)
                            .setTag(dbAndTable)
                            .setBody(serializedBytes)
                            .build();

                    try {
                        // 发送消息，需要关注发送结果，并捕获失败等异常。
                        SendReceipt sendReceipt = producer.send(message);
                        LOG.info("Send message successfully, messageId=" + sendReceipt.getMessageId() 
                        + "topic = " + hashTopic 
                        + "tag=" + dbAndTable);
                        // 发送成功后删除
                        records.remove(key);
                        
                    } catch (ClientException e) {
                        LOG.error("Failed to send message", e);
                    }
                }
            }
        }
    }


    private void processRecordMessage(MessageView messageView) {
        String dbAndTable = messageView.getTag().orElse(null);
        // dbname__tablename
        String dbName = dbAndTable.split("__")[0];
        String tableName = dbAndTable.split("__")[1];
        ByteBuffer body = messageView.getBody();

        Kryo kryo = kryoThreadLocal.get();
        byte[] byteArray = new byte[body.remaining()];

        // 使用 body 的只读缓冲区创建一个副本，并将其内容读入 byteArray
        body.duplicate().get(byteArray);

        // 将字节数组包装成 Input 对象
        Input input = new Input(byteArray);
        BinRecord binRecord;
        try {
            binRecord = kryo.readObject(input, BinRecord.class);
        } catch (Exception e) {
            LOG.error("Failed to deserialize record message");
            return;
        }

        System.out.println("Consume message successfully, messageId=" + messageView.getMessageId());
        String key = binRecord.getKey();
        if (localBinRecords.containsKey(dbAndTable)
                && localBinRecords.get(dbAndTable).containsKey(key)
                && localBinRecords.get(dbAndTable).get(key).getType() == binRecord.getType()) {
            localBinRecords.get(dbAndTable).remove(key);
            LOG.debug("Remove local record successfully, key=" + key);
        } else {
            compareService.addToRemoteBinRecords(dbName, tableName, key,
                    new TypeWithTime(binRecord.getTime(), binRecord.getType()));
            LOG.debug("Local record did not exist, key=" + key);
        }
    }


    /*
     * 处理 hash 消息
     * 将远端的hash存储到本地，用以后续的对比。
     * 这里存储使用的是一个先进先出的跳表，大小可以自定义。
     */
    public void processHashMessage(MessageView messageView) {
        System.out.println("Consume message successfully, messageId=" + messageView.getMessageId());
        String dbAndTable = messageView.getTag().orElse(null);
        // dbname__tablename
        String dbName = dbAndTable.split("__")[0];
        String tableName = dbAndTable.split("__")[1];
        ByteBuffer body = messageView.getBody();

        Kryo kryo = kryoThreadLocal.get();
        byte[] byteArray = new byte[body.remaining()];

        // 使用 body 的只读缓冲区创建一个副本，并将其内容读入 byteArray
        body.duplicate().get(byteArray);

        // 将字节数组包装成 Input 对象
        Input input = new Input(byteArray);
        HashWithTimestamp record;
        try {
            record = kryo.readObject(input, HashWithTimestamp.class);
        } catch (Exception e) {
            LOG.error("Failed to deserialize hash message");
            return;
        }
        compareService.addToRemoteHashs(dbAndTable, record.getTimestamp(), record.getHash());
        LOG.info(messageView.getMessageId() + "Store remote hash successfully, dbAndTable=" + dbAndTable + "hash=" + record);
    }

    public void printLocalBinRecords() {
        for (Map.Entry<String, ConcurrentHashMap<String, TypeWithTime>> entry : localBinRecords.entrySet()) {
            String dbAndTable = entry.getKey();
            ConcurrentHashMap<String, TypeWithTime> records = entry.getValue();
            for (Map.Entry<String, TypeWithTime> record : records.entrySet()) {
                String key = record.getKey();
                TypeWithTime typeWithTime = record.getValue();
                LOG.info("dbAndTable: " + dbAndTable + ", key: " + key + ", time: " + typeWithTime.getTime()
                        + ", type: " + typeWithTime.getType());
            }
        }
    }

    // 打印本地binlog记录中距离当前时间时间大于5s的记录
    public void printLocalBinRecordsWhereTimeRangeBiggerThan5s() {
        System.out.println("printLocalBinRecordsWhereTimeRangeBiggerThan5s:");
        for (Map.Entry<String, ConcurrentHashMap<String, TypeWithTime>> entry : localBinRecords.entrySet()) {
            String dbAndTable = entry.getKey();
            ConcurrentHashMap<String, TypeWithTime> records = entry.getValue();
            for (Map.Entry<String, TypeWithTime> record : records.entrySet()) {
                String key = record.getKey();
                TypeWithTime typeWithTime = record.getValue();
                long currentTime = System.currentTimeMillis();
                if (currentTime - typeWithTime.getTime() > 5000) {
                    LOG.info("dbAndTable: " + dbAndTable + ", key: " + key + ", time: " + typeWithTime.getTime()
                            + ", type: " + typeWithTime.getType());
                }
            }
        }
    }
}
