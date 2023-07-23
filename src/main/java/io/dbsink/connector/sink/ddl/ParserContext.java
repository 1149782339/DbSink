package io.dbsink.connector.sink.ddl;

import java.util.HashMap;
import java.util.Map;

/**
 * Parser Context, used by {@link io.dbsink.connector.sink.ddl.listener.MySqlToPGParserListener}
 *
 * @author Wang Wei
 * @time: 2023-07-22
 */
public class ParserContext {
    private final Map<String, Object> map = new HashMap<>();
    public <T> T get(String value) {
        return (T) map.get(value);
    }

    public void put(String key, Object value) {
        map.put(key, value);
    }
}
