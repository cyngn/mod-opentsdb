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

    public OpenTsDbSocketSender(String host, int port) {
        this.host = host;
        this.port = port;

        setupOpenTsdbConnection();
        if (openTsDbConnection == null) {
            throw new IllegalStateException("Cannot connect to openTsdb cluster host: " + host + " port: " + port);
        }
    }

    /**
     * Attempt to connect to OpenTsdb
     */
    private void setupOpenTsdbConnection() {
        try { close(); } catch (IOException ex) {}

        try {
            openTsDbConnection = new Socket(host, port);
            openTsDbConnection.setKeepAlive(true);
            metricsOutputStream = openTsDbConnection.getOutputStream();
        } catch (IOException ex) {
            logger.error("Failed to acquire connection to openTsDb host: " + host + " port: " + port + " ex:" + ex);
            openTsDbConnection = null;
            metricsOutputStream = null;
        }
    }

    /**
     * Allows the processor to send data to the OpenTsdb cluster, this method is only called from that thread
     *
     * @param data the raw data to send to OpenTsdb
     */
    @Override
    public void sendData(byte[] data) {
        if (openTsDbConnection != null && openTsDbConnection.isConnected()) {
            if (!sendDataOnStream(data)) {
                logger.error("Failed to write " + data.length + " bytes to openTsdb");
            }
        } else {
            logger.warn("Lost connection to openTsdb attempting to reconnect");
            // only try once then we stop trying and will try again next time around
            setupOpenTsdbConnection();
            if (!sendDataOnStream(data)) {
                logger.error("Failed to write " + data.length + " bytes to openTsdb");
            }
        }
    }

    private boolean sendDataOnStream(byte [] data) {
        boolean succeeded = true;
        try {
            metricsOutputStream.write(data);
        } catch (IOException ex) {
            logger.error("Failed to write metrics to host: " + host + " port: " + port +" ex: " + ex);
            succeeded = false;
        }
        return succeeded;
    }

    @Override
    public void close() throws IOException {
        if (metricsOutputStream != null) {
            try {
                metricsOutputStream.close();
                metricsOutputStream = null;
            } catch (IOException ex) { }
        }

        if (openTsDbConnection != null) {
            try {
                openTsDbConnection.close();
                openTsDbConnection = null;
            } catch (IOException ex) {}
        }
    }
}
