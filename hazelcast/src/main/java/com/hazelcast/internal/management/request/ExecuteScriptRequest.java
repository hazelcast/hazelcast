/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
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
 * limitations under the License.
 */

package com.hazelcast.internal.management.request;

import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.management.operation.ScriptExecutorOperation;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.util.AddressUtil;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.MapUtil;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.internal.util.JsonUtil.getArray;
import static com.hazelcast.internal.util.JsonUtil.getString;
import static com.hazelcast.internal.util.SetUtil.createHashSet;

/**
 * Request for executing scripts on the nodes from Management Center.
 */
public class ExecuteScriptRequest implements ConsoleRequest {

    private String script;
    private String engine;
    private Set<String> targets;

    public ExecuteScriptRequest() {
    }

    public ExecuteScriptRequest(String script, String engine, Set<String> targets) {
        this.script = script;
        this.engine = engine;
        this.targets = targets;
    }

    @Override
    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_EXECUTE_SCRIPT;
    }

    @Override
    public void writeResponse(ManagementCenterService mcs, JsonObject root) throws Exception {
        Map<Address, Future<Object>> futures = MapUtil.createHashMap(targets.size());

        for (String address : targets) {
            AddressUtil.AddressHolder addressHolder = AddressUtil.getAddressHolder(address);
            Address targetAddress = new Address(addressHolder.getAddress(), addressHolder.getPort());
            futures.put(targetAddress, mcs.callOnAddress(targetAddress, new ScriptExecutorOperation(engine, script)));
        }

        JsonObject responseJson = new JsonObject();
        for (Map.Entry<Address, Future<Object>> entry : futures.entrySet()) {
            Address address = entry.getKey();
            Future<Object> future = entry.getValue();

            try {
                addSuccessResponse(responseJson, address, prettyPrint(future.get()));
            } catch (ExecutionException e) {
                addErrorResponse(responseJson, address, e.getCause());
            } catch (InterruptedException e) {
                addErrorResponse(responseJson, address, e);
                Thread.currentThread().interrupt();
            }
        }

        root.add("result",  responseJson);
    }

    private String prettyPrint(Object result) {
        StringBuilder sb = new StringBuilder();
        if (result instanceof String) {
            sb.append(result);
        } else if (result instanceof List) {
            List list = (List) result;
            for (Object o : list) {
                sb.append(o).append("\n");
            }
        } else if (result instanceof Map) {
            Map map = (Map) result;
            for (Object o : map.entrySet()) {
                Map.Entry e = (Map.Entry) o;
                sb.append(e.getKey()).append("->").append(e.getValue()).append("\n");
            }
        } else if (result == null) {
            sb.append("error");
        }
        sb.append("\n");
        return sb.toString();
    }

    @Override
    public void fromJson(JsonObject json) {
        script = getString(json, "script", "");
        engine = getString(json, "engine", "");
        final JsonArray array = getArray(json, "targets", new JsonArray());
        targets = createHashSet(array.size());
        for (JsonValue target : array) {
            targets.add(target.asString());
        }
    }

    private static void addSuccessResponse(JsonObject root, Address address, String result) {
        addResponse(root, address, true, result, null);
    }

    private static void addErrorResponse(JsonObject root, Address address, Throwable e) {
        addResponse(root, address, false, e.getMessage(), ExceptionUtil.toString(e));
    }

    private static void addResponse(JsonObject root, Address address, boolean success, String result, String stackTrace) {
        JsonObject json = new JsonObject();
        json.add("success", success);
        json.add("result", result);
        json.add("stackTrace", stackTrace != null ? Json.value(stackTrace) : Json.NULL);
        root.add(address.toString(), json);
    }
}
