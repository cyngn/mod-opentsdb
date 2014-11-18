/*
 * Copyright 2014 Cyanogen Inc. All Rights Reserved.
 */
package com.cyngn.mods.opentsdb;

import org.junit.Test;
import org.vertx.java.core.json.JsonObject;

import static org.junit.Assert.assertEquals;

/**
 * @author truelove@cyngn.com (Jeremy Truelove) 11/10/14
 */
public class UtilTests {

    @Test
    public void testTags() {
        String tags = Util.createTagsFromJson(new JsonObject().putString("foo", "bar").putString("bar", "foo"));
        assertEquals(tags, "foo=bar bar=foo");
    }

    @Test
    public void testNoTags() {
        String tags = Util.createTagsFromJson(new JsonObject());
        assertEquals(tags, "");
    }
}
