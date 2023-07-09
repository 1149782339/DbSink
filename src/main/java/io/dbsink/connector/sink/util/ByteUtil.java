/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.util;

import java.util.ArrayList;
import java.util.List;

/**
 * Byte Util
 *
 * @author Wang Wei
 * @time: 2023-06-24
 */
public class ByteUtil {

    /**
     * byte array to hex string
     *
     * @param bytes     byte array
     * @param validBits valid bit number
     * @return hex string {@link String}
     */
    public static String bytesToBinaryString(byte[] bytes, int validBits) {
        List<Character> bits = new ArrayList<>(bytes.length * 8);
        for (int j = bytes.length - 1; j >= 0; j--) {
            for (int i = 7; i >= 0; i--) {
                int bit = (bytes[j] >> i) & 1;
                if (bits.isEmpty() && bit == 0) {
                    continue;
                }
                bits.add(bit == 0 ? '0' : '1');
            }
        }
        if (bits.isEmpty()) {
            bits.add('0');
        }
        if (bits.size() > validBits) {
            throw new IllegalArgumentException("exceed bit length " + validBits);
        }
        StringBuilder sb = new StringBuilder(validBits);
        for (int i = 0; i < validBits - bits.size(); i++) {
            sb.append('0');
        }
        for (Character bit : bits) {
            sb.append(bit);
        }
        return sb.toString();
    }

    /**
     * byte to boolean
     *
     * @param b byte
     * @return true or false {@link Boolean}
     */
    public static boolean byteToBoolean(byte b) {
        if (b == 1) {
            return Boolean.TRUE;
        } else if (b == 0) {
            return Boolean.FALSE;
        } else {
            throw new IllegalArgumentException("byte \"" + b + "\" can't be converted to boolean");
        }
    }
}
