/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.util;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.chrono.ChronoZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * Date time util
 *
 * @author Wang Wei
 * @time: 2023-06-24
 */
public class DateTimeUtil {
    private static final long NANOSECONDS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1);
    private static final long NANOSECONDS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);


    /**
     * Get {@link Timestamp} from nanosecond
     *
     * @param nanos nanosecond
     * @return the equivalent java sql Timestamp {@link Timestamp}
     */
    public static Timestamp toTimestamp(Long nanos) {
        return Optional.ofNullable(nanos)
            .map(n -> {
                Timestamp ts = new Timestamp(nanos / NANOSECONDS_PER_MILLISECOND);
                ts.setNanos((int) (nanos % NANOSECONDS_PER_SECOND));
                return ts;
            })
            .orElse(null);
    }

    /**
     * Get {@link Timestamp} from epoch with nano precision
     *
     * @param isoDT iso dateTime format "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS"
     * @param tz the timezone of the source database
     * @return the equivalent java sql Timestamp
     */
    public static Timestamp toTimestampFromIsoDateTime(String isoDT, TimeZone tz) {
        return Optional.ofNullable(isoDT)
            .map(i -> LocalDateTime.parse(isoDT, DateTimeFormatter.ISO_OFFSET_DATE_TIME))
            .map(t -> t.atZone(tz.toZoneId()))
            .map(ChronoZonedDateTime::toInstant)
            .map(Timestamp::from)
            .orElse(null);
    }
}
