package io.dbsink.connector.sink.ddl;

import io.dbsink.connector.sink.ddl.converters.ConversionConfiguration;
import io.dbsink.connector.sink.ddl.converters.ConversionResult;
import io.dbsink.connector.sink.ddl.converters.SQLConverter;
import io.dbsink.connector.sink.ddl.converters.SQLConverters;
import io.dbsink.connector.sink.dialect.DatabaseType;
import io.dbsink.connector.sink.naming.DefaultTableNamingStrategy;
import io.dbsink.connector.sink.naming.UpperCaseColumnNamingStrategy;
import io.dbsink.connector.sink.naming.UpperCaseTableNamingStrategy;
import io.dbsink.connector.sink.relation.TableId;
import io.dbsink.connector.sink.util.StringUtil;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestMySqlToPG {
    private static TableId parseTableId(String identifier) {
        Pattern pattern = Pattern.compile("(.+)(\\.(.+))?");
        Matcher matcher = pattern.matcher(identifier);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("sd");
        }
        String catalogName;
        String tableName;
        if (matcher.group(2) != null) {
            catalogName = StringUtil.unquote(matcher.group(1), '`');
            tableName = StringUtil.unquote(matcher.group(2), '`');
        } else {
            catalogName = null;
            tableName = StringUtil.unquote(matcher.group(1), '`');
        }
        return new TableId(catalogName, null, tableName);

    }

    public static String[] parseSQLIdentifier(String identifier, char delimiter) {
        boolean isInBlank = false;
        for (int i = 0; i < identifier.length(); i++) {
            char c = identifier.charAt(i);
            if (c == delimiter) {
                if (!isInBlank) {
                    isInBlank = true;
                } else if (i < identifier.length() - 1 && identifier.charAt(i + 1) == delimiter) {
                    i++;
                } else {
                    isInBlank = false;
                }
            }
        }
        String[] result = new String[2];

        Pattern pattern = Pattern.compile(delimiter + "([^" + delimiter + "]+)" + delimiter + "\\." + delimiter + "([^" + delimiter + "]+)" + delimiter);
        Matcher matcher = pattern.matcher(identifier);

        if (matcher.find()) {
            result[0] = matcher.group(1);
            result[1] = matcher.group(2);
        }

        return result;
    }

    public static void main(String[] args) {
        String identified = "`test`.`tat1`";
        String delimiter = "\"";
        String tmp = "\"\"";
        System.out.println(tmp.replace("\"\"", "\""));
        TableId tableId = parseTableId(identified);
        ConversionConfiguration configuration = new ConversionConfiguration(new UpperCaseTableNamingStrategy(), new UpperCaseColumnNamingStrategy());
        SQLConverter sqlConverter = SQLConverters.create(DatabaseType.MYSQL, DatabaseType.POSTGRES, configuration);

        String sql = "CREATE TABLE `auth_account` (\n" +
            "  `uid` bigint unsigned NOT NULL COMMENT '全平台用户唯一id',\n" +
            "  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',\n" +
            "  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',\n" +
            "  `username` varchar(30) CHARACTER SET utf8mb4 NOT NULL DEFAULT '' COMMENT '用户名',\n" +
            "  `password` varchar(64) CHARACTER SET utf8mb4  NOT NULL DEFAULT '' COMMENT '密码',\n" +
            "  `create_ip` varchar(15) CHARACTER SET utf8mb4  NOT NULL DEFAULT '' COMMENT '创建ip',\n" +
            "  `status` tinyint NOT NULL COMMENT '状态 1:启用 0:禁用 -1:删除',\n" +
            "  `sys_type` tinyint NOT NULL COMMENT '用户类型见SysTypeEnum 0.普通用户系统 1.商家端 2平台端',\n" +
            "  `user_id` bigint NOT NULL COMMENT '用户id',\n" +
            "  `tenant_id` bigint DEFAULT NULL COMMENT '所属租户',\n" +
            "  `is_admin` tinyint DEFAULT NULL COMMENT '是否是管理员',\n" +
            "  PRIMARY KEY (`uid`) USING BTREE,\n" +
            "  UNIQUE KEY `uk_usertype_userid` (`sys_type`,`user_id`) USING BTREE,\n" +
            "  KEY `idx_username` (`username`) USING BTREE\n" +
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='统一账户信息'";
       // String sql = "create table tmp(col1 int primary key auto_increment,col2 varchar(10), KEY `idx_attr` (`col2`) USING BTREE)";
        //String sql = "create table tmp(col1 int primary key,col2 varchar(10), KEY `idx_attr` (`col2`) USING BTREE)";
       // String sql = "alter table student add `chengji` int not null;";
        ConversionResult conversionResult = sqlConverter.convert(sql);
        for (String statement : conversionResult.getStatements()) {
            System.out.println(statement);
        }
        System.out.println("sdf");
    }
}
