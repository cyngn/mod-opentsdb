/*
 * Copyright 2014 Cyanogen Inc. All Rights Reserved.
 */
package com.cyngn.mods.opentsdb.client;

/**
 * Handles sending data to OpenTsDb
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 10/24/14
 */
public interface MetricsSender {
    void sendData(byte [] data);
}
