package com.hazelcast.internal.management.dto;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.internal.management.JsonSerializable;

import static com.hazelcast.util.JsonUtil.getInt;
import static com.hazelcast.util.JsonUtil.getLong;

/**
 * A Serializable DTO for {@link com.hazelcast.spi.impl.operationexecutor.slowoperationdetector.SlowOperationLog.Invocation}.
 */
public class SlowOperationInvocationDTO implements JsonSerializable {

    public int id;
    public long startedAt;
    public int durationMs;

    public SlowOperationInvocationDTO() {
    }

    public SlowOperationInvocationDTO(int id, long startedAt, int durationMs) {
        this.id = id;
        this.startedAt = startedAt;
        this.durationMs = durationMs;
    }

    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("id", id);
        root.add("startedAt", startedAt);
        root.add("durationMs", durationMs);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        id = getInt(json, "id");
        startedAt = getLong(json, "startedAt");
        durationMs = getInt(json, "durationMs");
    }
}
