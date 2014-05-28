package com.hazelcast.management;

import com.eclipsesource.json.JsonObject;

public interface JsonSerializable {

    JsonObject toJson();

    void fromJson(JsonObject json);
}
