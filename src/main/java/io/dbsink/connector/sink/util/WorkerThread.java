/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.util;

import io.dbsink.connector.sink.context.ConnectorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Worker Thread
 *
 * @author Wang Wei
 * @time: 2023-06-17
 */
public abstract class WorkerThread implements Runnable {

    private final static Logger LOGGER = LoggerFactory.getLogger(WorkerThread.class);

    private final static long MAX_AWAIT_TIME = 60;

    private volatile boolean isStopping;

    private final ExecutorService executor;

    protected final ErrorHandler errorHandler;

    protected final ConnectorContext context;

    public WorkerThread(ConnectorContext context, ErrorHandler errorHandler) {
        this.isStopping = false;
        this.executor = Executors.newSingleThreadExecutor();
        this.context = context;
        this.errorHandler = errorHandler;
    }

    /**
     * Start thread
     *
     * @author Wang Wei
     * @time: 2023-06-17
     */
    public synchronized void start() {
        executor.submit(this);
    }

    /**
     * Stop thread
     *
     * @author Wang Wei
     * @time: 2023-06-17
     */
    public synchronized void stop() {
        isStopping = false;
        executor.shutdownNow();
        try {
            executor.awaitTermination(TimeUnit.SECONDS.toSeconds(MAX_AWAIT_TIME), TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.debug("interrupted when stop,ignore it", e);
        }
    }

    /**
     * This method is called when thread is started
     * Thread main Subclasses of Thread should override this method.
     *
     * @author Wang Wei
     * @time: 2023-06-17
     */
    protected abstract void execute() throws Exception;

    @Override
    public final void run() {
        try {
            execute();
        } catch (Exception e) {
            if (isStopping) {
                LOGGER.trace("Worker thread is interrupted", e);
            } else {
                errorHandler.setThrowable(e);
            }
        }
    }
}
