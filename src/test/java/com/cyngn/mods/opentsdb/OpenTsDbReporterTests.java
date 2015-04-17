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

import org.junit.Ignore;
import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

import static org.vertx.testtools.VertxAssert.assertEquals;
import static org.vertx.testtools.VertxAssert.testComplete;

/**
 * Example Java integration test (You need to have OpenTsDb running)
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 11/10/14
 */
@Ignore("Integration tests, comment out annotation to run the tests")
public class OpenTsDbReporterTests extends TestVerticle {

    private EventBus eb;
    private static String topic = "test-opentsdb";

    @Override
    public void start() {
        eb = vertx.eventBus();
        JsonObject config = new JsonObject();
        config.putString("address", "test-opentsdb");
        JsonArray array = new JsonArray();
        array.add(new JsonObject().putString("host", "localhost").putNumber("port", 4242));
        config.putArray("hosts", array);
        config.putNumber("maxTags", 1);

        container.deployModule(System.getProperty("vertx.modulename"), config, 1, new AsyncResultHandler<String>() {
            @Override
            public void handle(AsyncResult<String> result) {
                if (result.succeeded()) {
                    OpenTsDbReporterTests.super.start();
                } else {
                    result.cause().printStackTrace();
                }
            }
        });
    }

    @Test
    public void testInvalidAction() throws Exception {
        JsonObject metric = new JsonObject();
        eb.send(topic, metric, (Message<JsonObject> result) -> {
            assertEquals("error", result.body().getString("status"));
        });

        metric = new JsonObject().putString("action", "badCommand");
        eb.send(topic, metric, (Message<JsonObject> result) -> {
            assertEquals("error", result.body().getString("status"));
            testComplete();
        });
    }

    @Test
    public void testNoTags() throws Exception {
        JsonObject metric = new JsonObject();
        metric.putString("action", OpenTsDbReporter.ADD_COMMAND);
        metric.putString("name", "test.value");
        metric.putString("value", "34.4");
        eb.send(topic, metric, (Message<JsonObject> result) -> {
            assertEquals("error", result.body().getString("status"));
            testComplete();
        });
    }

    @Test
    public void testSend() throws Exception {
        JsonObject metric = new JsonObject();
        metric.putString("action", OpenTsDbReporter.ADD_COMMAND);
        metric.putString("name", "test.value");
        metric.putString("value", "34.4");
        metric.putObject("tags", new JsonObject().putString("foo", "bar"));
        eb.send(topic, metric, (Message<JsonObject> result) -> {
            assertEquals("ok", result.body().getString("status"));
            testComplete();
        });
    }

    @Test
    public void testTooManyTags() throws Exception {
        JsonObject metric = new JsonObject();
        metric.putString("action", OpenTsDbReporter.ADD_COMMAND);
        metric.putString("name", "test.value");
        metric.putString("value", "34.4");
        metric.putObject("tags",
                         new JsonObject().putString("foo", "bar")
                                         .putString("var", "val"));
        eb.send(topic, metric, (Message<JsonObject> result) -> {
            assertEquals("error", result.body().getString("status"));
            testComplete();
        });
    }
}
