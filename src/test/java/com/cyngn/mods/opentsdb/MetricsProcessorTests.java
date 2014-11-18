/*
 * Copyright 2014 Cyanogen Inc. All Rights Reserved.
 */
package com.cyngn.mods.opentsdb;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

/**
 * @author truelove@cyngn.com (Jeremy Truelove) 11/10/14
 */
public class MetricsProcessorTests {

    @Before
    public void setUp(){
        MetricsProcessor.setupWorkload(1);
    }

    @Test
    public void testProcessing() {
        LinkedBlockingQueue<String> data = new LinkedBlockingQueue<>();
        String testStr = "aFake metric string";

        // add more data into queue
        data.add(testStr);
        data.add(testStr);

        final AtomicInteger count = new AtomicInteger(0);
        MetricsProcessor.processMetrics(data, testStr.getBytes().length * 3, (byteData) -> {
            count.incrementAndGet();
        });

        assertEquals(count.intValue(), 1);
    }

    @Test
    public void testMaxBuffer() {
        LinkedBlockingQueue<String> data = new LinkedBlockingQueue<>();
        String testStr = "aFake metric string";

        data.add(testStr);
        data.add(testStr);

        final AtomicInteger count = new AtomicInteger(0);
        MetricsProcessor.processMetrics(data, testStr.getBytes().length, (byteData) -> {
            count.incrementAndGet();
        });

        assertEquals(count.intValue(), 2);
    }

    @Test
    public void testMultipleWorkers() {
        LinkedBlockingQueue<String> data = new LinkedBlockingQueue<>();
        String testStr = "aFake metric string";

        data.add(testStr);
        data.add(testStr);
        data.add(testStr);
        data.add(testStr);

        MetricsProcessor.setupWorkload(2);

        final AtomicInteger count = new AtomicInteger(0);
        MetricsProcessor.processMetrics(data, (testStr.getBytes().length * 2) + 1, (byteData) -> {
            count.incrementAndGet();
        });

        assertEquals(count.intValue(), 1);

        // test that the first worker took half
        assertEquals(data.size(), 2);


    }
}
