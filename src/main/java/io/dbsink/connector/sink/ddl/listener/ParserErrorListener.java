package io.dbsink.connector.sink.ddl.listener;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.misc.ParseCancellationException;

/**
 * Parser error listener, throw exception when syntax error occurs
 *
 * @author Wang Wei
 * @time: 2023-07-22
 */
public class ParserErrorListener extends BaseErrorListener {
    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
        throw new ParseCancellationException("Syntax error at line " + line + ", position " + charPositionInLine + ": " + msg);
    }
}
