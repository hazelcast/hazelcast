package com.hazelcast.management;

import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;

public interface JsonSerializable {
    JsonValue toJson();
    void fromJson(JsonObject json);
}
