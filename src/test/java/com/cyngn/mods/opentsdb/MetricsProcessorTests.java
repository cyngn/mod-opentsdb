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
package com.cyngn.mods.opentsdb;

import com.cyngn.mods.opentsdb.client.MetricsSender;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

/**
 * @author truelove@cyngn.com (Jeremy Truelove) 11/10/14
 */
public class MetricsProcessorTests {

    private MetricsSender sender;
    private AtomicInteger count;

    @Before
    public void setUp(){
        MetricsProcessor.setupWorkload(1);
        count = new AtomicInteger(0);
        sender = new MetricsSender() {
            @Override
            public void sendData(byte[] data) {
                count.incrementAndGet();
            }

            @Override
            public boolean isConnected() {
                return true;
            }
        };
    }

    @Test
    public void testProcessing() {
        LinkedBlockingQueue<String> data = new LinkedBlockingQueue<>();
        String testStr = "aFake metric string";

        // add more data into queue
        data.add(testStr);
        data.add(testStr);
        MetricsProcessor.processMetrics(data, testStr.getBytes().length * 3, sender);
        assertEquals(count.intValue(), 1);
    }

    @Test
    public void testMaxBuffer() {
        LinkedBlockingQueue<String> data = new LinkedBlockingQueue<>();
        String testStr = "aFake metric string";

        data.add(testStr);
        data.add(testStr);

        MetricsProcessor.processMetrics(data, testStr.getBytes().length, sender);

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

        MetricsProcessor.processMetrics(data, (testStr.getBytes().length * 2) + 1, sender);

        assertEquals(count.intValue(), 1);

        // test that the first worker took half
        assertEquals(data.size(), 2);
    }
}
