/*
 * @Author: LHD
 * @Date: 2024-09-04 19:51:34
 * @LastEditors: 308twin 790816436@qq.com
 * @LastEditTime: 2024-09-09 15:16:10
 * @Description: 
 * 
 * Copyright (c) 2024 by 308twin@790816436@qq.com, All Rights Reserved. 
 */
package btree4j.utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.ZoneId;
//import btree4j.utils.lang.DateTimeFormatter;

public class Utils {
    private static MessageDigest md;
    private static final long FNV_PRIME = 0x100000001b3L;
    private static final long FNV_OFFSET_BASIS = 0xcbf29ce484222325L;
    private static long lastTime = 0;
    private static int sequence = 0;
    private static final int MAX_SEQUENCE = 9999;
    // 缓存 DateTimeFormatter 实例
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    public static long convertStringToLong(String dateString) {
        LocalDateTime localDateTime = LocalDateTime.parse(dateString, formatter);
        // 将 LocalDateTime 转为 long 类型的时间戳（以毫秒为单位）
        return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }
    static {
        try {
            // 只创建一次MessageDigest实例
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    

    public synchronized long generateUniqueTimestamp() {
        long currentTime = System.currentTimeMillis();

        if (currentTime == lastTime) {
            sequence++;
            if (sequence > MAX_SEQUENCE) {
                // 如果序列号超过最大值，等待下一个毫秒
                while (currentTime == lastTime) {
                    currentTime = System.currentTimeMillis();
                }
                sequence = 0;
            }
        } else {
            sequence = 0;
            lastTime = currentTime;
        }

        return currentTime * 10000 + sequence; // 扩展时间戳，包含序列号
    }

    public static String calculateMD5(String input) {
        // 确保每次计算前重置MessageDigest
        md.reset();

        // 更新摘要
        md.update(input.getBytes());

        // 计算MD5值
        byte[] digest = md.digest();

        // 将字节数组转换为16进制字符串
        StringBuilder sb = new StringBuilder();
        for (byte b : digest) {
            sb.append(String.format("%02x", b));
        }

        return sb.toString();
    }

    public static String fnv1aHash(String input) {
        long hash = FNV_OFFSET_BASIS;
        for (int i = 0; i < input.length(); i++) {
            hash ^= input.charAt(i);
            hash *= FNV_PRIME;
        }
        return Long.toHexString(hash);
    }
}
