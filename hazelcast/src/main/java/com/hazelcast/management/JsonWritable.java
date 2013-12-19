package com.hazelcast.management;

/**
 * User: sancar
 * Date: 18/12/13
 * Time: 11:55
 */
public interface JsonWritable {

    void toJson(JsonWriter writer);
}
