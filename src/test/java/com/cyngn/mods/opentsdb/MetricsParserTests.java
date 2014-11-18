/*
 * Copyright 2014 Cyanogen Inc. All Rights Reserved.
 */
package com.cyngn.mods.opentsdb;

import org.junit.Before;
import org.junit.Test;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.eventbus.impl.JsonObjectMessage;
import org.vertx.java.core.json.JsonObject;

import java.util.function.BiConsumer;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author truelove@cyngn.com (Jeremy Truelove) 11/11/14
 */
public class MetricsParserTests  {

    private BiConsumer<Message<JsonObject>, String> errorHandler;
    private Integer count;

    @Before
    public void setUp() {
        count = 0;
        errorHandler = (jsonObjectMessage, s) -> count++;
    }

    @Test
    public void missingNameTest() {
        JsonObject metric = new JsonObject();
        metric.putString("action", OpenTsDbReporter.ADD_COMMAND);
        metric.putString("value", "34.4");
        metric.putObject("tags", new JsonObject().putString("foo", "bar"));

        Message<JsonObject> msg = new JsonObjectMessage(false, "foo", metric);

        MetricsParser parser = new MetricsParser(null, "", errorHandler);
        String result = parser.createMetricString(msg);

        assertEquals(null, result);
        assertTrue(count == 1);
    }

    @Test
    public void missingValueTest() {
        JsonObject metric = new JsonObject();
        metric.putString("action", OpenTsDbReporter.ADD_COMMAND);
        metric.putString("name", "test.value");
        metric.putObject("tags", new JsonObject().putString("foo", "bar"));

        Message<JsonObject> msg = new JsonObjectMessage(false, "foo", metric);

        MetricsParser parser = new MetricsParser(null, "", errorHandler);
        String result = parser.createMetricString(msg);

        assertEquals(null, result);
        assertTrue(count == 1);
    }

    @Test
    public void missingTagsTest() {
        JsonObject metric = new JsonObject();
        metric.putString("action", OpenTsDbReporter.ADD_COMMAND);
        metric.putString("name", "test.value");
        metric.putString("value", "17");

        Message<JsonObject> msg = new JsonObjectMessage(false, "foo", metric);

        MetricsParser parser = new MetricsParser(null, "", errorHandler);
        String result = parser.createMetricString(msg);

        assertEquals(null, result);
        assertTrue(count == 1);
    }

    @Test
    public void defaultTagsTest() {
        JsonObject metric = new JsonObject();
        metric.putString("action", OpenTsDbReporter.ADD_COMMAND);
        metric.putString("name", "test.value");
        metric.putString("value", "17");

        Message<JsonObject> msg = new JsonObjectMessage(false, "foo", metric);

        MetricsParser parser = new MetricsParser(null, "foo=bar", errorHandler);
        String result = parser.createMetricString(msg);

        assertTrue(Pattern.compile("put test.value \\d* 17 foo=bar\\n").matcher(result).matches());
        assertTrue(count == 0);
    }

    @Test
    public void parseTestSimple() {
        JsonObject metric = new JsonObject();
        metric.putString("action", OpenTsDbReporter.ADD_COMMAND);
        metric.putString("name", "test.value");
        metric.putString("value", "17");
        metric.putObject("tags", new JsonObject().putString("tag1", "val1").putString("tag2", "val2"));

        Message<JsonObject> msg = new JsonObjectMessage(false, "foo", metric);

        MetricsParser parser = new MetricsParser(null, "foo=bar", errorHandler);
        String result = parser.createMetricString(msg);

        assertTrue(Pattern.compile("put test.value \\d* 17 foo=bar tag1=val1 tag2=val2\\n").matcher(result).matches());
        assertTrue(count == 0);
    }

    @Test
    public void parseTestNoDefaultTags() {
        JsonObject metric = new JsonObject();
        metric.putString("action", OpenTsDbReporter.ADD_COMMAND);
        metric.putString("name", "test.value");
        metric.putString("value", "17");
        metric.putObject("tags", new JsonObject().putString("tag1", "val1").putString("tag2", "val2"));

        Message<JsonObject> msg = new JsonObjectMessage(false, "foo", metric);

        MetricsParser parser = new MetricsParser(null, null, errorHandler);
        String result = parser.createMetricString(msg);

        assertTrue(Pattern.compile("put test.value \\d* 17 tag1=val1 tag2=val2\\n").matcher(result).matches());
        assertTrue(count == 0);
    }

    @Test
    public void parseTestPrefix() {
        JsonObject metric = new JsonObject();
        metric.putString("action", OpenTsDbReporter.ADD_COMMAND);
        metric.putString("name", "test.value");
        metric.putString("value", "17");
        metric.putObject("tags", new JsonObject().putString("tag1", "val1").putString("tag2", "val2"));

        Message<JsonObject> msg = new JsonObjectMessage(false, "foo", metric);

        MetricsParser parser = new MetricsParser("test.service", "foo=bar", errorHandler);
        String result = parser.createMetricString(msg);

        assertTrue(Pattern.compile("put test.service.test.value \\d* 17 foo=bar tag1=val1 tag2=val2\\n").matcher(result).matches());
        assertTrue(count == 0);
    }
}
