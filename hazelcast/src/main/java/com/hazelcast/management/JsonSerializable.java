package com.hazelcast.management;

import com.eclipsesource.json.JsonObject;

/**
 * JsonSerializable is a serialization interface that serializes/de-serializes to/from JSON.
 */
public interface JsonSerializable {

    JsonObject toJson();

    void fromJson(JsonObject json);
}
