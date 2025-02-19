package btree4j.server;

import java.io.FileOutputStream;
import java.nio.file.Files;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.nio.file.Path;
import org.springframework.stereotype.Service;
import javax.annotation.PostConstruct;
import java.io.File;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

@Service
public class SignatureService {
    @org.springframework.beans.factory.annotation.Value("${my.custom.config.isServer}")
    private boolean isServer;
    @org.springframework.beans.factory.annotation.Value("${my.custom.config.keyPath}")
    private String keyPath;

    private final DBService dbService;

    // 缓存密钥对
    private Map<String, PublicKey> publicKeyCache = new ConcurrentHashMap<>();
    private Map<String, PrivateKey> privateKeyCache = new ConcurrentHashMap<>();

    public SignatureService(DBService dbService) {
        this.dbService = dbService;
    }

    // 初始化密钥对
    @PostConstruct
    public void initKeyPairFromTables() throws Exception {
        if (keyPath == null || keyPath.isEmpty()) {
            throw new IllegalStateException(
                    "Key path is not properly configured. Please check application.yml configuration.");
        }

        List<String> tables = dbService.getAllTableNames();
        for (String tableName : tables) {
            // 尝试从 keyPath 读取密钥
            File privateKeyFile = new File(keyPath + "/" + tableName + "_private.key");
            File publicKeyFile = new File(keyPath + "/" + tableName + "_public.key");
            System.out.println("privateKeyFile: " + privateKeyFile);
            System.out.println("publicKeyFile: " + publicKeyFile);
            if (isServer) {
                // Server模式：判断公钥是否存在，不存在报错，存在则缓存公钥
                // if (!publicKeyFile.exists()) {
                //     throw new IllegalStateException("Public key not found for table: " + tableName);
                // } else {
                //     try {
                //         byte[] publicKeyBytes = Files.readAllBytes(publicKeyFile.toPath());
                //         KeyFactory keyFactory = KeyFactory.getInstance("EC");
                //         PublicKey publicKey = keyFactory.generatePublic(new X509EncodedKeySpec(publicKeyBytes));
                //         publicKeyCache.put(tableName, publicKey);
                //     } catch (Exception e) {
                //         e.printStackTrace();
                //     }
                // }
            } else {
                // Client模式：判断私钥是否存在，不存在就创建公私钥对，存在则缓存私钥
                if (!privateKeyFile.exists()) {
                    generateKeyPair(tableName);
                } else {
                    try {
                        byte[] privateKeyBytes = Files.readAllBytes(privateKeyFile.toPath());
                        KeyFactory keyFactory = KeyFactory.getInstance("EC");
                        PrivateKey privateKey = keyFactory.generatePrivate(new PKCS8EncodedKeySpec(privateKeyBytes));
                        privateKeyCache.put(tableName, privateKey);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    // 生成密钥对并保存到文件
    public void generateKeyPair(String tableName) {
        try {
            KeyPairGenerator keyGen = KeyPairGenerator.getInstance("EC");
            keyGen.initialize(256); // 使用常见的256位椭圆曲线
            KeyPair keyPair = keyGen.generateKeyPair();

            // 缓存需要的密钥
            if (isServer) {
                publicKeyCache.put(tableName, keyPair.getPublic());
            } else {
                privateKeyCache.put(tableName, keyPair.getPrivate());
            }

            // 获取私钥和公钥
            PrivateKey privateKey = keyPair.getPrivate();
            PublicKey publicKey = keyPair.getPublic();

            // 保存私钥到文件
            File privateKeyFile = new File(keyPath + "/" + tableName + "_private.key");
            try (FileOutputStream fos = new FileOutputStream(privateKeyFile)) {
                fos.write(privateKey.getEncoded());
            }

            // 保存公钥到文件
            File publicKeyFile = new File(keyPath + "/" + tableName + "_public.key");
            try (FileOutputStream fos = new FileOutputStream(publicKeyFile)) {
                fos.write(publicKey.getEncoded());
            }
        } catch (Exception e) {
            // 打印异常日志
            e.printStackTrace();
        }
    }

    // 使用私钥对数据进行签名
    public String signData(String tableName, byte[] data) {
        try {
            PrivateKey privateKey = privateKeyCache.get(tableName);
            if (privateKey == null) {
                throw new IllegalStateException("Private key not found for table: " + tableName);
            }
            Signature signature = Signature.getInstance("SHA256withECDSA");
            signature.initSign(privateKey);
            signature.update(data);
            byte[] signatureBytes = signature.sign();
            String signatureBase64 = Base64.getEncoder().encodeToString(signatureBytes);
            System.out.println("签名 (Base64): " + signatureBase64);
            return signatureBase64; // 使用 Base64 编码返回字符串
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    // 使用公钥验证签名
    public boolean verifySignature(String tableName, byte[] data, byte[] signatureBytes) {
        try {
            PublicKey publicKey = publicKeyCache.get(tableName);
            if (publicKey == null) {
                throw new IllegalStateException("Public key not found for table: " + tableName);
            }
            Signature signature = Signature.getInstance("SHA256withECDSA");
            signature.initVerify(publicKey);
            signature.update(data);
            return signature.verify(signatureBytes);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    // 获取指定表的公钥（Base64编码）
    public String getPublicKeyBase64(String tableName) {
        try {
            File publicKeyFile = new File(keyPath + "/" + tableName + "_public.key");
            if (!publicKeyFile.exists()) {
                throw new IllegalStateException("Public key not found for table: " + tableName);
            }
            byte[] publicKeyBytes = Files.readAllBytes(publicKeyFile.toPath());
            return Base64.getEncoder().encodeToString(publicKeyBytes);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    // 辅助方法：加载公钥
    private PublicKey loadPublicKey(Path path) throws Exception {
        byte[] publicKeyBytes = Files.readAllBytes(path);
        KeyFactory keyFactory = KeyFactory.getInstance("EC");
        return keyFactory.generatePublic(new X509EncodedKeySpec(publicKeyBytes));
    }

    // 辅助方法：加载私钥
    private PrivateKey loadPrivateKey(Path path) throws Exception {
        byte[] privateKeyBytes = Files.readAllBytes(path);
        KeyFactory keyFactory = KeyFactory.getInstance("EC");
        return keyFactory.generatePrivate(new PKCS8EncodedKeySpec(privateKeyBytes));
    }

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .disable(SerializationFeature.INDENT_OUTPUT)
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

    // 使用 ThreadLocal 缓存 TreeMap 实例
    private static final ThreadLocal<TreeMap<String, String>> threadLocalTreeMap = ThreadLocal
            .withInitial(TreeMap::new);

    // 验证数据库记录的签名
    public boolean verifyDatabaseRecord(String tableName, Map<String, Object> record, String signatureBase64) {
        try {
            // 获取线程内复用的 TreeMap 并清空旧数据
            TreeMap<String, String> canonicalJsonMap = threadLocalTreeMap.get();
            canonicalJsonMap.clear();

            // 过滤和标准化数据
            for (Map.Entry<String, Object> entry : record.entrySet()) {
                String columnName = entry.getKey();
                // 排除签名相关字段
                if ("signature".equalsIgnoreCase(columnName) || "verify_hash".equalsIgnoreCase(columnName)) {
                    continue;
                }
                // 统一转换字段值
                String value = entry.getValue() == null ? "" : entry.getValue().toString().trim();
                canonicalJsonMap.put(columnName, value);
            }

            // 生成规范的 JSON 字符串
            String canonicalJson = objectMapper.writeValueAsString(canonicalJsonMap);
            byte[] data = canonicalJson.getBytes(StandardCharsets.UTF_8);
            byte[] signatureBytes = Base64.getDecoder().decode(signatureBase64);

            // 验证签名
            return verifySignature(tableName, data, signatureBytes);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
}
