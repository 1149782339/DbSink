package io.dbsink.connector.sink.ddl.converters;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * DDL conversion message
 *
 * @author Wang Wei
 * @time: 2023-07-22
 */
public class ConversionMessage {
    private final List<String> warnings;
    private final List<String> infos;

    public ConversionMessage() {
        this.warnings = new ArrayList<>();
        this.infos = new ArrayList<>();
    }

    /**
     * Get conversion info messages
     *
     * @return conversion info messages
     * @author Wang Wei
     * @time: 2023-07-22
     */
    public List<String> getInfoMessages() {
        return Collections.unmodifiableList(infos);
    }

    /**
     * Get warning info messages
     *
     * @return conversion warning messages
     * @author Wang Wei
     * @time: 2023-07-22
     */
    public List<String> getWarningMessages() {
        return Collections.unmodifiableList(warnings);
    }


    /**
     * Put a message with the specified level
     *
     * @param  message conversion message
     * @param  level   message level
     * @author Wang Wei
     * @time: 2023-07-22
     */
    public void report(String message, Level level) {
        switch (level) {
            case INFO: {
                infos.add(message);
            }
            break;
            case WARNING: {
                warnings.add(message);
            }
            break;
            default:
                throw new IllegalArgumentException("message level is not valid");
        }
    }

    /**
     * Message level
     *
     * @author Wang Wei
     * @time: 2023-07-22
     */
    public enum Level {
        /**
         * Info level
         */
        INFO,
        /**
         * Warning level
         */
        WARNING
    }
}
