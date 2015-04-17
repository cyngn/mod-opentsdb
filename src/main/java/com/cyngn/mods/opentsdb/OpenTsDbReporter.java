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

import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

/**
 * Handles consuming metrics over the message bus, translating them into OpenTsDb metrics and queueing them up for send
 *  to the OpenTsDb cluster.
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 11/7/14
 */
public class OpenTsDbReporter extends BusModBase implements Handler<Message<JsonObject>> {

    public static final String ADD_COMMAND = "add";
    public static final int OPENTSDB_DEFAULT_MAX_TAGS = 8;

    private JsonArray hosts;
    private final int DEFAULT_MTU = 1500;
    private int maxBufferSizeInBytes;
    private BlockingQueue<String> metrics;
    private int maxTags;
    private int defaultTagCount;

    private Map<String, Consumer<Message<JsonObject>>> handlers;
    private List<MetricsWorker> workers;
    private String address;
    private MetricsParser metricsParser;

    @Override
    public void start(final Future<Void> startedResult) {
        super.start();

        hosts = getOptionalArrayConfig("hosts", new JsonArray("[{ \"host\" : \"localhost\", \"port\" : 4242}]"));
        maxBufferSizeInBytes = getOptionalIntConfig("maxBufferSizeInBytes", DEFAULT_MTU);
        String prefix = getOptionalStringConfig("prefix", null);
        address = getOptionalStringConfig("address", "vertx.opentsdb-reporter");
        maxTags = getOptionalIntConfig("maxTags", OPENTSDB_DEFAULT_MAX_TAGS);
        final JsonObject configuredTags = config.getObject("tags");
        if (configuredTags != null && configuredTags.size() > maxTags) {
            startedResult.setFailure(new IllegalStateException("Found more default tags than the max (" + maxTags + ")"));
        }
        defaultTagCount = configuredTags.size();
        String defaultTags = Util.createTagsFromJson(configuredTags);

        metricsParser = new MetricsParser(prefix, defaultTags, this::sendError);

        if (hosts.size() > 1) {
            MetricsProcessor.setupWorkload(hosts.size());
        }

        // create the list of workers
        workers = new ArrayList<>(hosts.size());
        metrics = new LinkedBlockingQueue<>();

        initializeWorkers();
        createMessageHandlers();

        eb.registerHandler(address, this);
    }

    private void initializeWorkers() {
        for (int i = 0; i < hosts.size(); i++) {
            JsonObject jsonHost = hosts.get(i);
            // we setup one worker dedicated to each endpoint, the same worker always rights to the same outbound socket
            MetricsWorker worker = new MetricsWorker(jsonHost.getString("host"), jsonHost.getInteger("port"), metrics,
                    maxBufferSizeInBytes);
            workers.add(worker);
            worker.start();
        }
    }

    @Override
    public void stop() {
        for (MetricsWorker worker : workers) {
            worker.stop();
        }
    }

    private void createMessageHandlers() {
        handlers = new HashMap<>();

        handlers.put(ADD_COMMAND, this::processMetric);
    }

    private void processMetric(Message<JsonObject> message) {
        final JsonObject tags = message.body().getObject("tags");
        if (tags != null && defaultTagCount + tags.size() > maxTags) {
            // the metric will be rejected by TSD, so don't even send it
            sendError(message, "You specified too many tags");
            return;
        }

        String metricStr = metricsParser.createMetricString(message);
        if (metricStr != null) {
            // put the metric in the work queue
            metrics.add(metricStr);
            sendOK(message);
        }
    }

    /**
     * Handles processing metric requests off the event bus
     *
     * @param message the metrics message
     */
    @Override
    public void handle(Message<JsonObject> message) {
        String action = message.body().getString("action");

        if (action == null ) {
            sendError(message, "You must specify an action");
        }

        Consumer<Message<JsonObject>> handler = handlers.get(action);

        if ( handler != null) {
            handler.accept(message);
        } else {
            sendError(message, "Invalid action: " + action + " specified.");
        }

    }
}
