package btree4j.service;

import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import btree4j.entity.BinRecord;
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

    @org.springframework.beans.factory.annotation.Value("${spring.datasource.url}")
    private String url;

    // private String dbName;

    private Producer producer; // 将 Producer 保持为全局的
    private ClientServiceProvider provider;
    private ClientConfiguration clientConfiguration;
    private PushConsumer pushConsumer;

    private static final ThreadLocal<Kryo> kryoThreadLocal = ThreadLocal.withInitial(Kryo::new);

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
                    .setTopics(recordTopic)
                    .setClientConfiguration(configuration)
                    .build();
        }

    }

    @PostConstruct
    public void initRecordConsumer() throws ClientException {
        if (!isServer) {
            provider = ClientServiceProvider.loadService();
            // 初始化 ClientConfiguration
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

            pushConsumer = provider.newPushConsumerBuilder()
                    .setClientConfiguration(clientConfiguration)
                    .setConsumerGroup("record_consumer") // 设置 Consumer Group
                    .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
                    .setMessageListener(messageView -> {
                        // LOG.info("Consume message successfully, messageId="+
                        // messageView.getMessageId());
                        processRecordMessage(messageView);
                        return ConsumeResult.SUCCESS;
                    })
                    .build();
        }

    }

    public void createMqTopic() throws MQClientException {
        // todo
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

    public void sendLocalHashsToRemote() {
        // todo
    }

    // 接收远程记录，并且将本地对应的记录删除
    // public void recieveRemoteRecords() {
    // if (!isServer) {
    // ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
    // .setEndpoints(proxyServerAddress)
    // .build();
    // String topic = recordTopic;
    // List<String> tags = compareService.getAllTableNames();
    // String tagString = String.join("||", tags);
    // FilterExpression filterExpression = new FilterExpression(tagString,
    // FilterExpressionType.TAG);
    // try (PushConsumer pushConsumer = provider.newPushConsumerBuilder()
    // .setClientConfiguration(clientConfiguration)
    // .setSubscriptionExpressions(Collections.singletonMap(topic,
    // filterExpression))
    // .setMessageListener(messageView -> {
    // LOG.info("Consume message successfully, messageId={}" +
    // messageView.getMessageId());
    // Optional<String> tableName = messageView.getTag();
    // ByteBuffer body = messageView.getBody();
    // // 反序列化
    // Kryo kryo = kryoThreadLocal.get();
    // Input input = new Input(body.array());
    // BinRecord binRecord = kryo.readObject(input, BinRecord.class);
    // // 将localBinRecords对应的记录删除
    // String key = binRecord.getKey();
    // if (localBinRecords.containsKey(tableName)
    // && localBinRecords.get(tableName).containsKey(key)
    // && localBinRecords.get(tableName).get(key).getTime() == binRecord.getTime()
    // && localBinRecords.get(tableName).get(key).getType() == binRecord.getType())
    // {
    // localBinRecords.get(tableName).remove(key);
    // LOG.debug("Remove local record successfully, key={}" + key);
    // } else {
    // LOG.error("Failed to remove local record, key={}" + key);
    // }
    // return ConsumeResult.SUCCESS;
    // })
    // .build()) {
    // } catch (ClientException | IOException e) {
    // e.printStackTrace();
    // }
    // }

    // }

    private void processRecordMessage(MessageView messageView) {
        Optional<String> dbAndTable = messageView.getTag();
        // dbname__tablename
        String dbName = dbAndTable.get().split("__")[0];
        String tableName = dbAndTable.get().split("__")[1];
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
                && localBinRecords.get(dbAndTable).get(key).getTime() == binRecord.getTime()
                && localBinRecords.get(dbAndTable).get(key).getType() == binRecord.getType()) {
            localBinRecords.get(dbAndTable).remove(key);
            LOG.debug("Remove local record successfully, key=" + key);
        } else {
            compareService.addToRemoteBinRecords(dbName, tableName, key,
                    new TypeWithTime(binRecord.getTime(), binRecord.getType()));
            LOG.debug("Local record did not exist, key=" + key);
        }
    }

    public void recieveRemoteHashs() {
        // todo
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
