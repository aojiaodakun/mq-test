package com.hzk.mq.support.util;

import java.nio.ByteBuffer;

/**
 * 类型转换工具类
 */
public class ClassCastUtil {

    public static long byteArrayToLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.put(bytes, 0, bytes.length);
        buffer.flip();
        return buffer.getLong();
    }

    public static byte[] longToBytes(long longVar) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(longVar).array();
    }

    public static int bytes2Int(byte[] bytes) {
        return bytes[0] & 0xFF | //
                (bytes[1] & 0xFF) << 8 | //
                (bytes[2] & 0xFF) << 16 | //
                (bytes[3] & 0xFF) << 24; //
    }

    public static byte[] int2bytes(int res) {
        byte[] targets = new byte[4];
        targets[0] = (byte) (res & 0xff);// 最低位
        targets[1] = (byte) ((res >> 8) & 0xff);// 次低位
        targets[2] = (byte) ((res >> 16) & 0xff);// 次高位
        targets[3] = (byte) (res >>> 24);// 最高位,无符号右移。
        return targets;
    }

}
