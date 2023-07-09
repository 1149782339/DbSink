/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.util;

import net.openhft.hashing.LongHashFunction;

/**
 * Hash util, used by write set {@link WriteSet}
 *
 * @author Wang Wei
 * @time: 2023-06-09
 */
public class HashUtil {
    private static final long DEFAULT_XX3_SEED = 199972221018L;
    private final LongHashFunction XX_3_HASH;

    public HashUtil(long seed) {
        XX_3_HASH = LongHashFunction.xx3(seed);
    }

    public HashUtil() {
        XX_3_HASH = LongHashFunction.xx3(DEFAULT_XX3_SEED);
    }

    public long getHash(String value) {
        return XX_3_HASH.hashChars(value);
    }
}
