package io.dbsink.connector.sink.ddl.listener;

import io.dbsink.connector.sink.ddl.parser.mysql.MySqlParser;

import java.util.HashMap;
import java.util.Map;

/**
 * Mapping of data types between MySQL and PostgreSQL, used by {@link MySqlToPGParserListener}
 *
 * @author Wang Wei
 * @time: 2023-07-22
 */
public class MySqlDataTypeToPGMap {
    private static Map<Integer, String> map = new HashMap<>();
    static {
        map.put(MySqlParser.SMALLINT, "SMALLINT");
        map.put(MySqlParser.YEAR, "SMALLINT");
        map.put(MySqlParser.MEDIUMBLOB, "BYTEA");
        map.put(MySqlParser.BIGINT, "BIGINT");
        map.put(MySqlParser.INTEGER, "INTEGER");
        map.put(MySqlParser.LONGBLOB, "BYTEA");
        map.put(MySqlParser.MEDIUMBLOB, "BYTEA");
        map.put(MySqlParser.DATE, "DATE");
        map.put(MySqlParser.BLOB, "BYTEA");
        map.put(MySqlParser.TINYBLOB, "BYTEA");
        map.put(MySqlParser.VARBINARY, "BYTEA");
        map.put(MySqlParser.BINARY, "BYTEA");
        map.put(MySqlParser.TEXT, "TEXT");
        map.put(MySqlParser.MEDIUMTEXT, "TEXT");
        map.put(MySqlParser.LONGTEXT, "TEXT");
        map.put(MySqlParser.BOOLEAN, "BOOLEAN");
        map.put(MySqlParser.INT, "int");
    }
    public static String get(Integer dataType) {
        return map.get(dataType);
    }

    public static boolean contains(Integer dataType) {
        return map.containsKey(dataType);
    }
}
