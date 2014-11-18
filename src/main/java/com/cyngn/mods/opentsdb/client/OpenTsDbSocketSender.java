/*
 * Copyright 2014 Cyanogen Inc. All Rights Reserved.
 */
package com.cyngn.mods.opentsdb.client;

import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

/**
 * Handles sending the data to OpenTsDb via a direct socket connection.
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 11/10/14
 */
public class OpenTsDbSocketSender implements MetricsSender, Closeable {
    private final Logger logger = LoggerFactory.getLogger(OpenTsDbSocketSender.class);
    private Socket openTsDbConnection;
    private OutputStream metricsOutputStream;
    private final String host;
    private final int port;

    // opentsdb often disconnects clients that don't send for a period, we can only reliably detect disconnection on
    //  failed writes, so we need to do basic retry
    private static int MAX_SEND_ATTEMPTS = 2;

    public OpenTsDbSocketSender(String host, int port) {
        this.host = host;
        this.port = port;

        if (!setupOpenTsdbConnection()) {
            throw new IllegalStateException("Cannot connect to openTsdb cluster host: " + host + " port: " + port);
        }
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
            metricsOutputStream = openTsDbConnection.getOutputStream();
            logger.info("Connected to host: " + host + " port: " + port);
        } catch (IOException ex) {
            logger.error("Failed to acquire connection to openTsDb host: " + host + " port: " + port + " ex:" + ex);
            openTsDbConnection = null;
            metricsOutputStream = null;
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
        } catch (IOException ex) {
            succeeded = false;
            silentClose();
        }
        return succeeded;
    }

    /**
     * Used to clean up the old socket so we can re-initialize it
     */
    private void silentClose() {
        try {
            close();
        } catch (IOException ex) {}
    }

    @Override
    public void close() throws IOException {
        if (metricsOutputStream != null) {
            try {
                metricsOutputStream.close();
                metricsOutputStream = null;
            } catch (IOException ex) {
                metricsOutputStream = null;
            }
        }

        if (openTsDbConnection != null) {
            try {
                openTsDbConnection.close();
                openTsDbConnection = null;
            } catch (IOException ex) {
                openTsDbConnection = null;
            }
        }
    }
}
