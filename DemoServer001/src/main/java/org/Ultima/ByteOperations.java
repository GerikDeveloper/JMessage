package org.Ultima;

import java.nio.charset.StandardCharsets;

/***
 * Bytes Operations Class for Server version Demo_0.0.1
 ***/
public class ByteOperations {
    public static int ByteToInt(byte x) {
        return (x < 0) ? 256 + x : x;
    }

    public static byte IntToByte(int x) {
        return (byte) ((x < 0) ? 128 + x : x);
    }

    public static byte[] Get_ByteString_UTF_8(String s) {
        byte[] Bytes = ("_" + s).getBytes(StandardCharsets.UTF_8);
        Bytes[0] = IntToByte(Bytes.length - 1);
        return Bytes;
    }

    public static byte[] Get_IntString_UTF_8(String s) {
        byte[] Bytes = ("____" + s).getBytes(StandardCharsets.UTF_8);
        byte[] BytesLength = IntToBytes(Bytes.length - 4);
        Bytes[0] = BytesLength[0];
        Bytes[1] = BytesLength[1];
        Bytes[2] = BytesLength[2];
        Bytes[3] = BytesLength[3];
        return Bytes;
    }

    public static byte[] Get_Bytes_By_String_UTF_8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    public static String Get_String_From_IntString_UTF_8(byte[] IntString) {
        return new String(IntString, StandardCharsets.UTF_8).substring(4);
    }

    public static String Get_String_From_ByteString_UTF_8(byte[] ByteString) {
        String result = new String(ByteString, StandardCharsets.UTF_8);
        return result.substring(1);
    }

    public static String Get_String_UTF_8(byte[] Bytes, int length) {
        String result = new String(Bytes, StandardCharsets.UTF_8);
        return result.substring(0, length);
    }

    public static String Get_String_UTF_8(byte[] Bytes) {
        return new String(Bytes, StandardCharsets.UTF_8);
    }

    public static int BytesToInt(byte[] Bytes) {
        int[] Integers = new int[4];
        Integers[0] = ByteToInt(Bytes[0]);
        Integers[1] = ByteToInt(Bytes[1]) << 8;
        Integers[2] = ByteToInt(Bytes[2]) << 16;
        Integers[3] = ByteToInt(Bytes[3]) << 24;
        return Integers[0] | Integers[1] | Integers[2] | Integers[3];
    }

    public static long BytesToLong(byte[] Bytes) {
        long[] Longs = new long[8];
        Longs[0] = ByteToInt(Bytes[0]);
        Longs[1] = ((long) ByteToInt(Bytes[1])) << 8;
        Longs[2] = ((long) ByteToInt(Bytes[1])) << 16;
        Longs[3] = ((long) ByteToInt(Bytes[1])) << 24;
        Longs[4] = ((long) ByteToInt(Bytes[1])) << 32;
        Longs[5] = ((long) ByteToInt(Bytes[1])) << 40;
        Longs[6] = ((long) ByteToInt(Bytes[1])) << 48;
        Longs[7] = ((long) ByteToInt(Bytes[1])) << 56;
        return Longs[0] | Longs[1] | Longs[2] | Longs[3] | Longs[4] | Longs[5] | Longs[6] | Longs[7];
    }

    public static String GetStringByIp(byte[] Ip) {
        if (Ip.length == 4) {
            return ByteToInt(Ip[0]) + "." + ByteToInt(Ip[1]) + "." + ByteToInt(Ip[2]) + "." + ByteToInt(Ip[3]);
        } else {
            StringBuilder resultBuilder = new StringBuilder();
            for (byte Byte : Ip) {
                resultBuilder.append(ByteToInt(Byte)).append(":");
            }
            String result = resultBuilder.toString();
            return result.substring(0, result.length() - 1);
        }
    }

    public static byte[] IntToBytes(int Integer) {
        int[] Integers = new int[4];
        Integers[0] = (Integer << 24) >> 24;
        Integers[1] = (Integer << 16) >> 24;
        Integers[2] = (Integer << 8) >> 24;
        Integers[3] = (Integer >> 24);
        byte[] result = new byte[4];
        result[0] = IntToByte(Integers[0]);
        result[1] = IntToByte(Integers[1]);
        result[2] = IntToByte(Integers[2]);
        result[3] = IntToByte(Integers[3]);
        return result;
    }

    public static byte[] LongToBytes(long num) {
        long[] nums = new long[8];
        nums[0] = (num << 56) >> 56;
        nums[1] = (num << 48) >> 56;
        nums[2] = (num << 40) >> 56;
        nums[3] = (num << 32) >> 56;
        nums[4] = (num << 24) >> 56;
        nums[5] = (num << 16) >> 56;
        nums[6] = (num << 8) >> 56;
        nums[7] = (num >> 56);
        byte[] result = new byte[8];
        result[0] = IntToByte((int) nums[0]);
        result[1] = IntToByte((int) nums[1]);
        result[2] = IntToByte((int) nums[2]);
        result[3] = IntToByte((int) nums[3]);
        result[4] = IntToByte((int) nums[4]);
        result[5] = IntToByte((int) nums[5]);
        result[6] = IntToByte((int) nums[6]);
        result[7] = IntToByte((int) nums[7]);
        return result;
    }
}
