/*
 * Copyright 2014 Cyanogen Inc. All Rights Reserved.
 */
package com.cyngn.mods.opentsdb;

import org.vertx.java.core.json.JsonObject;

/**
 * General purpose utils
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 11/10/14
 */
public class Util {

    /**
     * Take config specified tags and make a OpenTsDb tag string
     *
     * @param tags the map of tags to convert to their string form
     * @return list of tags in opentsdb format ie 'name1=value1 name2=value2'
     */
    public static String createTagsFromJson(JsonObject tags) {
        String tagsString = "";
        if (tags != null && tags.size() > 0) {
            StringBuilder builder = new StringBuilder();
            for (String key : tags.getFieldNames()) {
                builder.append(key).append("=").append(tags.getString(key)).append(" ");
            }
            // grab all but th
            tagsString = builder.substring(0, builder.length() -1);
        }

        return tagsString;
    }
}
