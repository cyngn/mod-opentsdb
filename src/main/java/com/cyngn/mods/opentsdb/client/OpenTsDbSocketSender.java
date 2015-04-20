/*
 * Copyright 2014 Cyanogen Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.cyngn.mods.opentsdb.client;

import org.joda.time.DateTime;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Handles sending the data to OpenTsDb via a direct socket connection.
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 11/10/14
 */
public class OpenTsDbSocketSender implements MetricsSender, Closeable {
    private final Logger logger = LoggerFactory.getLogger(OpenTsDbSocketSender.class);
    private Socket openTsDbConnection;
    private OutputStream metricsOutputStream;
    private InputStream metricsInputStream;
    private final String host;
    private final int port;
    private AtomicInteger disconnectCountForReportingPeriod;
    private AtomicInteger connectCount;
    private AtomicInteger successfulSendCount;
    private final int FIVE_MINUTES_IN_MILLI = 1000 * 60 * 5;
    private final int FIVE_SECONDS_IN_MILLI = 5000;
    private AtomicBoolean inQuietPeriod;
    private byte [] readBuffer = new byte[1024 * 10];
    private DateTime lastRead;
    private int TEN_SECONDS = 10;
    private int MAX_DISCONNECTS_BEFORE_QUIET_PERIOD = 5;
    private int READ_TIMEOUT_MILLI = 500;


    // opentsdb often disconnects clients that don't send for a period, we can only reliably detect disconnection on
    //  failed writes, so we need to do basic retry
    private static int MAX_SEND_ATTEMPTS = 2;
    private AtomicInteger disconnectCountForQuietPeriod;

    public OpenTsDbSocketSender(String host, int port) {
        this.host = host;
        this.port = port;
        disconnectCountForReportingPeriod = new AtomicInteger(0);
        connectCount = new AtomicInteger(0);
        successfulSendCount = new AtomicInteger(0);
        disconnectCountForQuietPeriod = new AtomicInteger(0);
        lastRead = DateTime.now();

        if (!setupOpenTsdbConnection()) {
            throw new IllegalStateException("Cannot connect to openTsdb cluster host: " + host + " port: " + port);
        }

        inQuietPeriod = new AtomicBoolean(false);
        Timer disconnectTimer = initializeDisconnectTracker();
        Timer quietPeriodTimer = initializeQuietPeriodDetection();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                disconnectTimer.cancel();
                quietPeriodTimer.cancel();
            }
        });
    }

    private Timer initializeDisconnectTracker() {
        Timer disconnectTracker = new Timer();
        disconnectTracker.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                int connectedTimes = connectCount.getAndSet(0);
                int errorCount = disconnectCountForReportingPeriod.getAndSet(0);
                int successCount = successfulSendCount.getAndSet(0);
                if (errorCount > 0) {
                    logger.error("Observed " + errorCount + " disconnects and " + connectedTimes +" connects, with " +
                            successCount + " successful sends");
                } else {
                    logger.info("Sent to OpenTsDB " + successCount +" times without disconnect");
                }
            }
        }, 0, FIVE_MINUTES_IN_MILLI);
        return disconnectTracker;
    }

    private Timer initializeQuietPeriodDetection() {
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                int disconnects = disconnectCountForQuietPeriod.getAndSet(0);
                if (disconnects > MAX_DISCONNECTS_BEFORE_QUIET_PERIOD) {
                    inQuietPeriod.set(true);
                    disconnectCountForQuietPeriod.set(0);
                    logger.error("Entering quiet period had " + disconnects + " disconnects in a 5 second period");
                } else if (!inQuietPeriod.compareAndSet(false, false)) {
                    logger.warn("Exiting quiet period!!!!");
                }
            }
        }, 0, FIVE_SECONDS_IN_MILLI);

        return timer;
    }

    /**
     * Attempt to connect to OpenTsdb
     */
    private boolean setupOpenTsdbConnection() {
        // ensure we don't have old socket state around
        silentClose();

        boolean connectionCreated = true;
        try {
            openTsDbConnection = new Socket(host, port);
            openTsDbConnection.setKeepAlive(true);
            openTsDbConnection.setTcpNoDelay(true);
            openTsDbConnection.setSoTimeout(READ_TIMEOUT_MILLI); // set the reads to time out in half a second without data
            metricsInputStream = openTsDbConnection.getInputStream();
            metricsOutputStream = openTsDbConnection.getOutputStream();
            logger.info("Connected to host: " + host + " port: " + port);
            connectCount.incrementAndGet();
        } catch (IOException ex) {
            logger.error("Failed to acquire connection to openTsDb host: " + host + " port: " + port + " ex:" + ex);
            openTsDbConnection = null;
            metricsOutputStream = null;
            metricsInputStream = null;
            connectionCreated = false;
        }

        return connectionCreated;
    }

    /**
     * Allows the processor to send data to the OpenTsdb cluster, this method is only called from that thread
     *
     * @param data the raw data to send to OpenTsdb
     */
    @Override
    public void sendData(byte[] data) {
        sendDataInternal(data, 1);
    }

    private void sendDataInternal(byte[] data, int attempts) {
        if(inQuietPeriod.get()) {
            logger.error("Throwing away " + data.length + " bytes of metrics due to quiet period");
            return;
        }

        if (attempts > MAX_SEND_ATTEMPTS) {
            // close the socket and give up for now
            silentClose();
            logger.error("Failed to write " + data.length + " bytes to openTsdb");
            return;
        }

        // as long as it appears we are connected attempt to send the data
        if (openTsDbConnection != null && metricsOutputStream != null) {
            // if the send fails the other side of the socket shut us down, try again
            if (!sendDataOnStream(data)) {
                setupOpenTsdbConnection();
                sendDataInternal(data, ++attempts);
            }
        } else if (attempts == 1) {
            logger.warn("Lost connection to openTsdb attempting to reconnect and send");
            // only try once then we stop trying and will try again next time around, in case of an actual outage
            if (!setupOpenTsdbConnection() || !sendDataOnStream(data)) {
                logger.error("Failed to write " + data.length + " bytes to openTsdb");
            }
        }
    }

    private boolean sendDataOnStream(byte [] data) {
        boolean succeeded = true;
        try {
            metricsOutputStream.write(data);
            successfulSendCount.incrementAndGet();
            checkWriteErrors();
        } catch (IOException ex) {
            disconnectCountForReportingPeriod.incrementAndGet();
            disconnectCountForQuietPeriod.incrementAndGet();
            succeeded = false;
            silentClose();
            logger.error("Got exception ex: ", ex);
        }
        return succeeded;
    }

    /**
     * This is here to relieve pressure on the OpenTsDb error write buffers, if you don't do this open tsdb agents
     *  outbound buffers can fill and cause OOM exceptions.
     */
    private void checkWriteErrors() throws IOException {
        if (DateTime.now().minusSeconds(TEN_SECONDS).isAfter(lastRead)) {
            lastRead = DateTime.now();
            try {
                int count = metricsInputStream.read(readBuffer);
                if (count > 0) {
                    logger.error("Got error response from OpenTsDb, error: " + new String(readBuffer, 0, count));
                }
                if (count == -1) {
                    // this is the end of the file, which means no more data will ever come
                    //  not on this read or the next.  here we should close the socket.
                    throw new IOException("saw EOF on the socket, probably closed");
                }
            } catch (SocketTimeoutException ste) {
                logger.debug("No errors read from OpenTsDb in the last " + TEN_SECONDS + "(s)");
            } catch (IOException ex) {
                throw ex;
            }
        }
    }

    /**
     * Used to clean up the old socket so we can re-initialize it
     */
    private void silentClose() {
        try { close(); } catch (IOException ex) { }
    }

    @Override
    public void close() throws IOException {
        if (metricsInputStream != null) {
            try {
                metricsInputStream.close();
                metricsInputStream = null;
            } catch (IOException ex) {
                metricsInputStream = null;
                logger.error("close - encountered problems closing metricsInputStream ex: ", ex);
            }
        }

        if (metricsOutputStream != null) {
            try {
                metricsOutputStream.close();
                metricsOutputStream = null;
            } catch (IOException ex) {
                metricsOutputStream = null;
                logger.error("close - encountered problems closing metricsOutputStream ex: ", ex);
            }
        }

        if (openTsDbConnection != null) {
            try {
                openTsDbConnection.close();
                openTsDbConnection = null;
            } catch (IOException ex) {
                logger.error("close - encountered problems closing socket ex: ", ex);
                openTsDbConnection = null;
            }
        }
    }

    @Override
    public boolean isConnected() {
        return openTsDbConnection != null && metricsOutputStream != null;
    }
}
