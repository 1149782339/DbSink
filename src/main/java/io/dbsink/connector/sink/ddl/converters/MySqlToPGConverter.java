package io.dbsink.connector.sink.ddl.converters;

import io.dbsink.connector.sink.ddl.CaseChangingCharStream;
import io.dbsink.connector.sink.ddl.listener.MySqlToPGParserListener;
import io.dbsink.connector.sink.ddl.listener.ParserErrorListener;
import io.dbsink.connector.sink.ddl.parser.mysql.MySqlLexer;
import io.dbsink.connector.sink.ddl.parser.mysql.MySqlParser;
import io.dbsink.connector.sink.dialect.DatabaseType;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.util.concurrent.CancellationException;

/**
 * MySql to PostgreSQL DDL SQL converter
 *
 * @author Wang Wei
 * @time: 2023-07-22
 */
public class MySqlToPGConverter extends CommonSQLConverter {

    public MySqlToPGConverter(ConversionConfiguration configuration) {
        super(configuration);
    }

    @Override
    public ConversionResult convert(String statement) {
        CodePointCharStream ddlContentCharStream = CharStreams.fromString(statement);
        Lexer lexer = new MySqlLexer(new CaseChangingCharStream(ddlContentCharStream, true));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        MySqlParser parser = new MySqlParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(new ParserErrorListener());
        ParseTree parseTree = parser.root();
        ParseTreeWalker parseTreeWalker = new ParseTreeWalker();
        MySqlToPGParserListener parserListener = new MySqlToPGParserListener(tokens, configuration);
        ConversionStatus status = ConversionStatus.SUCCEEDED;
        String error = "";
        try {
            parseTreeWalker.walk(parserListener, parseTree);
        } catch (CancellationException e) {
            status = ConversionStatus.FAILED;
            error = e.getMessage();
        }
        ConversionMessage message = parserListener.getMessage();
        return ConversionResult.builder()
            .statements(parserListener.getStatements())
            .status(status)
            .infos(message.getInfoMessages())
            .warnings(message.getWarningMessages())
            .error(error)
            .build();
    }

    public static class MySqlToPGConverterProvider implements SQLConverterProvider {

        @Override
        public DatabaseType sourceDatabase() {
            return DatabaseType.MYSQL;
        }

        @Override
        public DatabaseType targetDatabase() {
            return DatabaseType.POSTGRES;
        }

        @Override
        public SQLConverter create(ConversionConfiguration configuration) {
            return new MySqlToPGConverter(configuration);
        }
    }

}
