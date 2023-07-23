package io.dbsink.connector.sink.ddl.converters;

import java.util.Collections;
import java.util.List;

/**
 * DDL Conversion Result
 *
 * @author Wang Wei
 * @time: 2023-07-22
 */
public class ConversionResult {
    private final ConversionStatus status;
    private final List<String> warnings;
    private final List<String> infos;
    private final String error;
    private final List<String> statements;

    public ConversionResult(
        List<String> statements,
        ConversionStatus status,
        List<String> infos,
        List<String> warnings,
        String error
    ) {
        this.statements = statements;
        this.status = status;
        this.infos = infos;
        this.warnings = warnings;
        this.error = error;
    }

    /**
     * Get the converted SQL statements.
     * Remember one statement from the source database may be converted into
     * multiple statements in the target database, it's a one-to-many relationship.
     *
     * @return sql statements
     * @author Wang Wei
     * @time: 2023-07-22
     */
    public List<String> getStatements() {
        return Collections.unmodifiableList(statements);
    }

    /**
     * Get conversion status
     *
     * @return conversion status {@link ConversionStatus}
     * @author Wang Wei
     * @time: 2023-07-22
     */
    public ConversionStatus getStatus() {
        return status;
    }

    /**
     * Get conversion warning messages
     *
     * @return conversion warning messages
     * @author Wang Wei
     * @time: 2023-07-22
     */
    public List<String> getWarnings() {
        return Collections.unmodifiableList(warnings);
    }


    /**
     * Get conversion info messages
     *
     * @return conversion info messages
     * @author Wang Wei
     * @time: 2023-07-22
     */
    public List<String> getInfos() {
        return Collections.unmodifiableList(infos);
    }

    /**
     * Get conversion error messages
     *
     * @return conversion error messages
     * @author Wang Wei
     * @time: 2023-07-22
     */
    public String getErrors() {
        return error;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private ConversionStatus status;
        private List<String> warnings;
        private List<String> infos;
        private String error;
        private List<String> statements;

        public Builder status(ConversionStatus status) {
            this.status = status;
            return this;
        }

        public Builder warnings(List<String> warnings) {
            this.warnings = warnings;
            return this;
        }

        public Builder infos(List<String> infos) {
            this.infos = infos;
            return this;
        }

        public Builder error(String error) {
            this.error = error;
            return this;
        }

        public Builder statements(List<String> statements) {
            this.statements = statements;
            return this;
        }

        public ConversionResult build() {
            return new ConversionResult(statements, status, infos, warnings, error);
        }
    }

}
