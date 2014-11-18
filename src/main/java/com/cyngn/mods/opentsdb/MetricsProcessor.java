/*
 * Copyright 2014 Cyanogen Inc. All Rights Reserved.
 */
package com.cyngn.mods.opentsdb;

import com.cyngn.mods.opentsdb.client.MetricsSender;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * Handles chunking metric data into optimal sizes to OpenTsdb
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 11/10/14
 */
public class MetricsProcessor {
    private static final Logger logger = LoggerFactory.getLogger(MetricsProcessor.class);

    private static boolean fullyDrainQueue = true;
    protected static int maxWorkers = 1;

    /**
     * Set the number of workers the processor should assume are working against the queue.
     *
     * NOTE:  we want package protected here as the main reporter will set this, we don't want it accessible from the
     *    outside
     *
     * @param workers total # of endpoints we are sending to
     */
    static void setupWorkload(int workers) {
        if (workers < 1) {
            throw new IllegalArgumentException("You can't ever have less than one worker");
        }

        if (workers == 1) {
            fullyDrainQueue = true;
        } else {
            fullyDrainQueue = false;
            maxWorkers = workers;
        }
    }

    /**
     * Given a queue of metrics to send, process the metrics into the right format and send them over a socket
     *
     * @param metrics the metrics queue to work off
     * @param maxBufferSizeInBytes the maximum amount to send on any socket send
     * @param metricSender the socket sender to use
     */
    public static void processMetrics(BlockingQueue<String> metrics, int maxBufferSizeInBytes,
                                      MetricsSender metricSender) {

        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        List<String> drainedMetrics = new ArrayList<>();
        try {
            // this call blocks so we don't sit and constantly poll for data
            drainedMetrics.add(metrics.take());
        } catch (InterruptedException ex) {
            logger.error("Failed to drain metric queue, ex: ", ex);
            return;
        }

        // if we have one worker fully drain the backlog, otherwise split it between the workers in a naive way
        if (fullyDrainQueue) {
            metrics.drainTo(drainedMetrics);
        } else {
            // subsequent workers will take less but theoretically should complete faster and re-enter and grab more
            metrics.drainTo(drainedMetrics, (int)Math.ceil(metrics.size() / maxWorkers));
        }

        // loop through and serialize the metrics and send them as we fill the buffer up to max buffer
        for (String metric : drainedMetrics) {
            byte[] bytes = metric.getBytes();

            // if this would exceed the max buffer to send go ahead and pass to the sender
            if (bytes.length + stream.size() > maxBufferSizeInBytes) {
                // this is a blocking socket call we only need one sender
                metricSender.sendData(stream.toByteArray());
                stream.reset();
            }

            try {
                stream.write(bytes);
            } catch (IOException ex) {
                logger.error("Failed to write to stream, ex: ", ex);
                // if we failed lets just give up on this batch
                stream.reset();
            }
        }

        // send whatever is left in the buffer
        if (stream.size() > 0) {
            metricSender.sendData(stream.toByteArray());
            stream.reset();
        }
    }
}
