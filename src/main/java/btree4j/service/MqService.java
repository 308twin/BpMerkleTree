package btree4j.service;

import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import btree4j.entity.BinRecord;
import btree4j.entity.ConcurrentLimitedSortedStore;
import btree4j.entity.HashWithTimestamp;
import btree4j.entity.TypeWithTime;
import btree4j.server.DBService;
import btree4j.server.SignatureService;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.PreDestroy;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

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
    private PushConsumer signaturePushConsumer;
    private static final ThreadLocal<Kryo> kryoThreadLocal = ThreadLocal.withInitial(Kryo::new);

    private ConcurrentHashMap<String, ConcurrentHashMap<String, TypeWithTime>> remoteBinRecords;
    private ConcurrentHashMap<String, ConcurrentHashMap<String, TypeWithTime>> localBinRecords;
    private ConcurrentHashMap<String, ConcurrentLimitedSortedStore> localHashs;
    private ConcurrentHashMap<String, ConcurrentLimitedSortedStore> aboutToSendHashs;
    private ConcurrentHashMap<String, ConcurrentLimitedSortedStore> remoteHashs;
    private CompareService compareService;
    private DBService dbService;
    private SignatureService signatureService;

    // 修改线程池定义
    private final ScheduledExecutorService scheduledExecutor = new ScheduledThreadPoolExecutor(
        Runtime.getRuntime().availableProcessors() * 2,
        new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger(1);
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, "SignatureProcessor-" + counter.getAndIncrement());
                thread.setDaemon(true);
                return thread;
            }
        },
        new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                // 当任务被拒绝时，在调用线程中直接执行
                LOG.warn("Thread pool overloaded, executing task in caller thread");
                r.run();
            }
        }
    );

    public MqService(ConcurrentHashMap<String, ConcurrentHashMap<String, TypeWithTime>> remoteBinRecords,
            ConcurrentHashMap<String, ConcurrentHashMap<String, TypeWithTime>> localBinRecords,
            @Qualifier("localHashs") ConcurrentHashMap<String, ConcurrentLimitedSortedStore> localHashs,
            @Qualifier("remoteHashs") ConcurrentHashMap<String, ConcurrentLimitedSortedStore> remoteHashs,
            @Qualifier("aboutToSendHashs") ConcurrentHashMap<String, ConcurrentLimitedSortedStore> aboutToSendHashs,
            CompareService compareService,
            DBService dbService,
            SignatureService signatureService) {
        this.remoteBinRecords = remoteBinRecords;
        this.localBinRecords = localBinRecords;
        this.localHashs = localHashs;
        this.remoteHashs = remoteHashs;
        this.aboutToSendHashs = aboutToSendHashs;
        this.compareService = compareService;
        this.dbService = dbService;
        this.signatureService = signatureService;
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

    @PostConstruct
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
                    .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
                    .setMessageListener(messageView -> {
                        // LOG.debug("Consume message successfully, messageId="+
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

    @PostConstruct
    public void initSignatureConsumer() throws ClientException {
        if (isServer) {
            provider = ClientServiceProvider.loadService();
            clientConfiguration = ClientConfiguration.newBuilder()
                    .setEndpoints(proxyServerAddress)
                    .build();

            // 初始化 PushConsumer
            String topic = "signature";
            
            // 使用 * 作为通配符接收所有消息
            FilterExpression filterExpression = new FilterExpression("*", FilterExpressionType.TAG);

            signaturePushConsumer = provider.newPushConsumerBuilder()
                    .setClientConfiguration(clientConfiguration)
                    .setConsumerGroup("signature_consumer")
                    .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
                    .setMessageListener(messageView -> {
                        processSignatureMessage(messageView);
                        return ConsumeResult.SUCCESS;
                    })
                    .build();
            LOG.info("Init signature consumer successfully with wildcard tag subscription");
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
                        // LOG.info("Send message successfully, messageId=" + sendReceipt.getMessageId()
                        // + " tag=" + dbAndTable);
                        // 发送成功后删除
                        records.remove(key);
                    } catch (ClientException e) {
                        LOG.error("Failed to send message", e);
                    }
                }
            }
        }
    }
    public void sendSignatureToRemote(String canonicalJson, String txId, String dbAndTable)
            throws ClientException, IOException {
        if (!isServer) {
            String signature = signatureService.signData(dbAndTable, canonicalJson.getBytes(StandardCharsets.UTF_8));
            LOG.info("Start to send signature");
            // Send signature to remote
            HashMap<String, String> signatureMap = new HashMap<>(); // 明确使用HashMap类型
            signatureMap.put("signature", signature);
            signatureMap.put("txId", txId);
            Kryo kryo = kryoThreadLocal.get();
            ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
            Output output = new Output(byteOut);
            byteOut.reset();
            kryo.writeObject(output, signatureMap); // 序列化具体的HashMap类型
            output.flush();
            byte[] serializedBytes = byteOut.toByteArray();
            
            LOG.info("Serialized signature map: " + signatureMap); // 添加日志
            
            Message message = provider.newMessageBuilder()
                    .setTopic("signature")
                    .setTag(dbAndTable)
                    .setBody(serializedBytes)
                    .build();
            try {
                SendReceipt sendReceipt = producer.send(message);
                LOG.info("Sent signature message successfully, messageId=" + sendReceipt.getMessageId());
            } catch (ClientException e) {
                LOG.error("Failed to send signature message", e);
                throw e;
            }
        }
    }

    public void processSignatureMessage(MessageView messageView) {
        if (isServer) {
            try {
                LOG.info("Processing signature message, messageId=" + messageView.getMessageId());
                ByteBuffer body = messageView.getBody();
                byte[] byteArray = new byte[body.remaining()];
                body.get(byteArray);
        
                Kryo kryo = kryoThreadLocal.get();
                Input input = new Input(byteArray);
                HashMap<String, String> signatureMap = kryo.readObject(input, HashMap.class);
                
                LOG.info("Deserialized signature map: " + signatureMap);
                
                String signature = signatureMap.get("signature");
                String txId = signatureMap.get("txId");
                String dbAndTable = messageView.getTag().orElse(null);
                
                if (signature == null || txId == null || dbAndTable == null) {
                    LOG.error("Invalid signature message: signature=" + signature + ", txId=" + txId + ", dbAndTable=" + dbAndTable);
                    return;
                }
                
                // 使用线程池延迟执行更新操作
                final String finalSignature = signature;
                final String finalTxId = txId;
                final String finalDbAndTable = dbAndTable;
                
                scheduledExecutor.schedule(() -> {
                    try {
                        dbService.updateSignature(finalSignature, finalTxId, finalDbAndTable);
                        LOG.info("Successfully processed signature message for dbAndTable=" + finalDbAndTable);
                    } catch (Exception e) {
                        LOG.error("Failed to update signature", e);
                    }
                }, 3, TimeUnit.SECONDS);
                
            } catch (Exception e) {
                LOG.error("Failed to deserialize signature message", e);
                e.printStackTrace();
            }
        }
    }

    // 发送本地hash记录到远程
    public void sendLocalHashsToRemote() {
        if (isServer) {
            // 遍历localHashRecords，构建消息，发送到proxyServer,发送后删除
            for (Map.Entry<String, ConcurrentLimitedSortedStore> entry : aboutToSendHashs.entrySet()) {
                String dbAndTable = entry.getKey();
                ConcurrentLimitedSortedStore records = entry.getValue();
                Kryo kryo = kryoThreadLocal.get();
                ByteArrayOutputStream byteOut = new ByteArrayOutputStream(); // 重用字节输出流
                Output output = new Output(byteOut); // 重用 Kryo 的 Output 对象
                for (Map.Entry<String, Long> record : records.entrySet()) {
                    String key = record.getKey();
                    Long value = record.getValue();
                    byteOut.reset();

                    // 写入record
                    HashWithTimestamp hashWithTimestamp = new HashWithTimestamp(key, value);
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
                        LOG.info("Send local hash successfully, messageId=" + sendReceipt.getMessageId()
                                + " topic = " + hashTopic
                                + " tag=" + dbAndTable
                                + " hash=" + value);
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

        LOG.debug("Consume record message successfully, messageId=" + messageView.getMessageId());
        String key = binRecord.getKey();
        compareService.addToRemoteBinRecords(dbName, tableName, key,
                new TypeWithTime(binRecord.getTime(), binRecord.getType()));
        LOG.debug("Local record did not exist, key=" + key);
    }

    /*
     * 处理 hash 消息
     * 将远端的hash存储到本地，用以后续的对比。
     * 这里存储使用的是一个先进先出的跳表，大小可以自定义。
     */
    public void processHashMessage(MessageView messageView) {
        LOG.debug("Consume message successfully, messageId=" + messageView.getMessageId());
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
        LOG.info("Store remote hash successfully, dbAndTable=" + dbAndTable + "hash=" + record);
    }

    public void printLocalBinRecords() {
        for (Map.Entry<String, ConcurrentHashMap<String, TypeWithTime>> entry : localBinRecords.entrySet()) {
            String dbAndTable = entry.getKey();
            ConcurrentHashMap<String, TypeWithTime> records = entry.getValue();
            for (Map.Entry<String, TypeWithTime> record : records.entrySet()) {
                String key = record.getKey();
                TypeWithTime typeWithTime = record.getValue();
                LOG.debug("dbAndTable: " + dbAndTable + ", key: " + key + ", time: " + typeWithTime.getTime()
                        + ", type: " + typeWithTime.getType());
            }
        }
    }

    // 打印本地binlog记录中距离当前时间时间大于5s的记录
    public void printLocalBinRecordsWhereTimeRangeBiggerThan5s() {
        LOG.debug("printLocalBinRecordsWhereTimeRangeBiggerThan5s:");
        for (Map.Entry<String, ConcurrentHashMap<String, TypeWithTime>> entry : localBinRecords.entrySet()) {
            String dbAndTable = entry.getKey();
            ConcurrentHashMap<String, TypeWithTime> records = entry.getValue();
            for (Map.Entry<String, TypeWithTime> record : records.entrySet()) {
                String key = record.getKey();
                TypeWithTime typeWithTime = record.getValue();
                long currentTime = System.currentTimeMillis();
                if (currentTime - typeWithTime.getTime() > 5000) {
                    LOG.debug("dbAndTable: " + dbAndTable + ", key: " + key + ", time: " + typeWithTime.getTime()
                            + ", type: " + typeWithTime.getType());
                }
            }
        }
    }

    // 在类的销毁方法中添加线程池关闭逻辑
    @PreDestroy
    public void destroy() {
        try {
            scheduledExecutor.shutdown();
            if (!scheduledExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduledExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduledExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
