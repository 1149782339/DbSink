/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink;

import io.dbsink.connector.sink.naming.*;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.lang.ref.PhantomReference;
import java.time.ZoneId;
import java.util.Map;

/**
 * Connector Config
 *
 * @author: Wang Wei
 * @time: 2023-06-17
 */
public class ConnectorConfig extends AbstractConfig {
    private static final String DATABASE_DIALECT_GROUP = "DatabaseDialect";

    private final static String DATABASE_DIALECT_NAME = "database.dialect.name";

    private static final String CONNECTION_GROUP = "Connection";

    private final static String CONNECTION_USERNAME = "connection.username";

    private final static String CONNECTION_PASSWORD = "connection.password";

    private final static String CONNECTION_URL = "connection.url";

    private static final String CONNECTION_URL_DOC = "connection connection url";

    private static final String CONNECTION_URL_DISPLAY = "connection connection url";

    private final static String CONNECTION_DRIVER_CLASS = "connection.driver.class";

    private final static String CONNECTION_RETRIES_MAX = "connection.retries.max";

    private final static int CONNECTION_RETRIES_MAX_DEFAULT = 5;


    private final static String CONNECTION_BACKOFF_MS = "connection.backoff.ms";

    private final static int CONNECTION_BACKOFF_MS_DEFAULT = 3000;

    private static final String APPLIER_GROUP = "applier";

    private static final String APPLIER_PARALLEL_MAX = "applier.parallel.max";

    private static final String APPLIER_TRANSACTION_ENABLED = "applier.transaction.enabled";

    private static final boolean APPLIER_TRANSACTION_ENABLED_DEFAULT = false;

    private static final String APPLIER_DDL_ENABLED = "applier.ddl.enabled";

    private static final String APPLIER_WORKER_BUFFER_SIZE = "applier.worker.buffer.size";

    private static final int APPLIER_WORKER_BUFFER_SIZE_DEFAULT = 100;

    private static final boolean APPLIER_DDL_ENABLED_DEFAULT = false;

    private static int APPLIER_PARALLEL_MAX_DEFAULT = (int) (Runtime.getRuntime().availableProcessors() * 1.4);

    private static final String APPLIER_TRANSACTION_BUFFER_SIZE = "applier.transaction.buffer.size";

    private static int TRANSACTION_BUFFER_SIZE_DEFAULT = 20;

    private static final String APPLIER_TIMEZONE = "applier.timezone";

    private static final String APPLIER_TIMEZONE_DEFAULT = "UTC";

    private static String TABLE_NAMING_STRATEGY = "table.naming.strategy";

    private static String COLUMN_NAMING_STRATEGY = "column.naming.strategy";

    public final static ConfigDef CONFIG_DEF = new ConfigDef()
        .define(
            CONNECTION_URL,
            ConfigDef.Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            ConfigDef.Importance.HIGH,
            CONNECTION_URL_DOC,
            CONNECTION_GROUP,
            1,
            ConfigDef.Width.LONG,
            CONNECTION_URL_DISPLAY
        )
        .define(
            CONNECTION_USERNAME,
            ConfigDef.Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            ConfigDef.Importance.HIGH,
            CONNECTION_URL_DOC,
            CONNECTION_GROUP,
            2,
            ConfigDef.Width.LONG,
            CONNECTION_URL_DISPLAY
        )
        .define(
            CONNECTION_PASSWORD,
            ConfigDef.Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            ConfigDef.Importance.HIGH,
            CONNECTION_URL_DOC,
            CONNECTION_GROUP,
            3,
            ConfigDef.Width.LONG,
            CONNECTION_URL_DISPLAY
        )
        .define(
            CONNECTION_DRIVER_CLASS,
            ConfigDef.Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            ConfigDef.Importance.HIGH,
            CONNECTION_URL_DOC,
            CONNECTION_GROUP,
            3,
            ConfigDef.Width.LONG,
            CONNECTION_URL_DISPLAY
        )
        .define(
            CONNECTION_RETRIES_MAX,
            ConfigDef.Type.INT,
            CONNECTION_RETRIES_MAX_DEFAULT,
            ConfigDef.Importance.HIGH,
            CONNECTION_URL_DOC,
            CONNECTION_GROUP,
            3,
            ConfigDef.Width.LONG,
            CONNECTION_URL_DISPLAY
        )
        .define(
            CONNECTION_BACKOFF_MS,
            ConfigDef.Type.INT,
            CONNECTION_BACKOFF_MS_DEFAULT,
            ConfigDef.Importance.HIGH,
            CONNECTION_URL_DOC,
            CONNECTION_GROUP,
            3,
            ConfigDef.Width.LONG,
            CONNECTION_URL_DISPLAY
        )
        .define(
            DATABASE_DIALECT_NAME,
            ConfigDef.Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            ConfigDef.Importance.HIGH,
            CONNECTION_URL_DOC,
            DATABASE_DIALECT_GROUP,
            4,
            ConfigDef.Width.LONG,
            CONNECTION_URL_DISPLAY
        )
        .define(
            APPLIER_PARALLEL_MAX,
            ConfigDef.Type.INT,
            APPLIER_PARALLEL_MAX_DEFAULT,
            ConfigDef.Importance.HIGH,
            CONNECTION_URL_DOC,
            APPLIER_GROUP,
            4,
            ConfigDef.Width.LONG,
            CONNECTION_URL_DISPLAY
        )
        .define(
            APPLIER_TRANSACTION_BUFFER_SIZE,
            ConfigDef.Type.INT,
            TRANSACTION_BUFFER_SIZE_DEFAULT,
            ConfigDef.Importance.HIGH,
            CONNECTION_URL_DOC,
            APPLIER_GROUP,
            4,
            ConfigDef.Width.LONG,
            CONNECTION_URL_DISPLAY
        )
        .define(
            APPLIER_TRANSACTION_ENABLED,
            ConfigDef.Type.BOOLEAN,
            APPLIER_TRANSACTION_ENABLED_DEFAULT,
            ConfigDef.Importance.HIGH,
            CONNECTION_URL_DOC,
            APPLIER_GROUP,
            4,
            ConfigDef.Width.LONG,
            CONNECTION_URL_DISPLAY
        )
        .define(
            APPLIER_DDL_ENABLED,
            ConfigDef.Type.BOOLEAN,
            APPLIER_DDL_ENABLED_DEFAULT,
            ConfigDef.Importance.HIGH,
            CONNECTION_URL_DOC,
            APPLIER_GROUP,
            4,
            ConfigDef.Width.LONG,
            CONNECTION_URL_DISPLAY
        )
       .define(
            APPLIER_WORKER_BUFFER_SIZE,
            ConfigDef.Type.INT,
            APPLIER_WORKER_BUFFER_SIZE_DEFAULT,
            ConfigDef.Importance.HIGH,
            CONNECTION_URL_DOC,
            APPLIER_GROUP,
           4,
            ConfigDef.Width.LONG,
            CONNECTION_URL_DISPLAY
        )
        .define(
            TABLE_NAMING_STRATEGY,
            ConfigDef.Type.CLASS,
            DefaultTableNamingStrategy.class,
            ConfigDef.Importance.HIGH,
            CONNECTION_URL_DOC,
            APPLIER_GROUP,
            4,
            ConfigDef.Width.LONG,
            CONNECTION_URL_DISPLAY
        )
        .define(
            COLUMN_NAMING_STRATEGY,
            ConfigDef.Type.CLASS,
            DefaultColumnNamingStrategy.class,
            ConfigDef.Importance.HIGH,
            CONNECTION_URL_DOC,
            APPLIER_GROUP,
            4,
            ConfigDef.Width.LONG,
            CONNECTION_URL_DISPLAY
        );
    //RELATION_NAMING_STRATEGY
    private final String connectionUrl;

    private final String connectionUsername;

    private final String connectionPassword;

    private final String connectionDriverClass;

    private final int connectionRetriesMax;

    private final int connectionBackoff;

    private final String databaseDialectName;

    private final int transactionBufferSize;

    private final int applierParallel;

    private final boolean transactionEnabled;

    private final boolean applierDDLEnabled;

    private final int applierWorkerBufferSize;

    private final TableNamingStrategy tableNamingStrategy;

    private final ColumnNamingStrategy columnNamingStrategy;

    public ConnectorConfig(Map<?, ?> originals) {
        super(CONFIG_DEF, originals);
        connectionUrl = getString(CONNECTION_URL);
        connectionUsername = getString(CONNECTION_USERNAME);
        connectionPassword = getString(CONNECTION_PASSWORD);
        databaseDialectName = getString(DATABASE_DIALECT_NAME);
        connectionDriverClass = getString(CONNECTION_DRIVER_CLASS);
        connectionRetriesMax = getInt(CONNECTION_RETRIES_MAX);
        transactionBufferSize = getInt(APPLIER_TRANSACTION_BUFFER_SIZE);
        applierParallel = getInt(APPLIER_PARALLEL_MAX);
        transactionEnabled = getBoolean(APPLIER_TRANSACTION_ENABLED);
        applierDDLEnabled = getBoolean(APPLIER_DDL_ENABLED);
        tableNamingStrategy = getConfiguredInstance(TABLE_NAMING_STRATEGY, TableNamingStrategy.class);
        columnNamingStrategy = getConfiguredInstance(COLUMN_NAMING_STRATEGY, ColumnNamingStrategy.class);
        connectionBackoff = getInt(CONNECTION_BACKOFF_MS);
        applierWorkerBufferSize = getInt(APPLIER_WORKER_BUFFER_SIZE);
    }

    public String getConnectionUrl() {
        return connectionUrl;
    }

    public String getConnectionUsername() {
        return connectionUsername;
    }

    public String getConnectionPassword() {
        return connectionPassword;
    }

    public String getConnectionDriverClass() {
        return connectionDriverClass;
    }

    public int getConnectionRetriesMax() {
        return connectionRetriesMax;
    }

    public int getConnectionBackoff() {
        return connectionBackoff;
    }

    public String getDatabaseDialectName() {
        return databaseDialectName;
    }

    public int getTransactionBufferSize() {
        return transactionBufferSize;
    }

    public int getApplierParallelMax() {
        return applierParallel;
    }

    public boolean getTransactionEnabled() {
        return transactionEnabled;
    }

    public boolean getApplierDDLEnabled() {
        return applierDDLEnabled;
    }

    public int getApplierWorkerBufferSize() {
        return applierWorkerBufferSize;
    }

    public TableNamingStrategy getTableNamingStrategy() {
        return tableNamingStrategy;
    }

    public ColumnNamingStrategy getColumnNamingStrategy() {
        return columnNamingStrategy;
    }
}
