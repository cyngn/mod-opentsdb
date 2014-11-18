/*
 * Copyright 2014 Cyanogen Inc. All Rights Reserved.
 */
package com.cyngn.mods.opentsdb;

import com.cyngn.mods.opentsdb.client.OpenTsDbSocketSender;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Handles processing work to send to an openTsDb endpoint
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 11/10/14
 */
public class MetricsWorker {

    private static Logger logger = LoggerFactory.getLogger(MetricsWorker.class);

    private final ThreadPoolExecutor pool;
    private final String host;
    private final int port;
    private final BlockingQueue<String> workQueue;
    private final int maxBufferSizeInBytes;
    private OpenTsDbSocketSender metricSender;
    private AtomicBoolean running;

    public MetricsWorker(String host, int port, BlockingQueue<String> queue, int maxBufferSizeInBytes) {
        this.host = host;
        this.port = port;
        this.workQueue = queue;
        this.maxBufferSizeInBytes = maxBufferSizeInBytes;

        pool = new ThreadPoolExecutor(1, 1, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        running = new AtomicBoolean(false);
    }

    /**
     * Starts a thread dedicated to sending data to an OpenTsDb endpoint
     */
    public void start() {
        if (pool.isShutdown()) {
            throw new IllegalStateException("Worker was already started and stopped");
        }

        // this will attempt to connect  the socket immediately
        metricSender = new OpenTsDbSocketSender(host, port);

        running = new AtomicBoolean(true);
        // start the processor
        pool.submit((Runnable) () -> {
            logger.info("Starting metrics processing...");
            // this call blocks on an empty queue otherwise it always is running and draining the metrics to send
            while (running.get()) {
                try {
                    MetricsProcessor.processMetrics(workQueue, maxBufferSizeInBytes, metricSender);
                } catch (Exception ex) {
                    logger.error("Encountered processing error, ex:", ex);
                }
            }
            logger.info("Stopping metrics processing...");
        });

    }

    /**
     * Stops the socket sending
     */
    public void stop() {
        running.set(false);

        pool.shutdown();
        try {
            if (metricSender != null) {
                metricSender.close();
                metricSender = null;
            }
        } catch (IOException e) {}
    }
}
