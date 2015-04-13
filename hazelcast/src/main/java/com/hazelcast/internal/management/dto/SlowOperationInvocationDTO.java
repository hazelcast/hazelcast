package com.hazelcast.internal.management.dto;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.internal.management.JsonSerializable;

import static com.hazelcast.util.JsonUtil.getInt;
import static com.hazelcast.util.JsonUtil.getLong;
import static com.hazelcast.util.JsonUtil.getString;

/**
 * A Serializable DTO for {@link com.hazelcast.spi.impl.operationexecutor.slowoperationdetector.SlowOperationLog.Invocation}.
 */
public class SlowOperationInvocationDTO implements JsonSerializable {

    public int id;
    public String operationDetails;
    public long startedAt;
    public int durationMs;

    public SlowOperationInvocationDTO() {
    }

    public SlowOperationInvocationDTO(int id, String operationDetails, long startedAt, int durationMs) {
        this.id = id;
        this.operationDetails = operationDetails;
        this.startedAt = startedAt;
        this.durationMs = durationMs;
    }

    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("id", id);
        root.add("details", operationDetails);
        root.add("startedAt", startedAt);
        root.add("durationMs", durationMs);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        id = getInt(json, "id");
        operationDetails = getString(json, "details");
        startedAt = getLong(json, "startedAt");
        durationMs = getInt(json, "durationMs");
    }
}
