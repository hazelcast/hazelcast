/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.management.request;

import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import com.hazelcast.core.Member;
import com.hazelcast.management.ManagementCenterService;
import com.hazelcast.management.operation.ScriptExecutorOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.util.AddressUtil;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.util.JsonUtil.getArray;
import static com.hazelcast.util.JsonUtil.getBoolean;
import static com.hazelcast.util.JsonUtil.getString;

public class ExecuteScriptRequest implements ConsoleRequest {

    private String script;
    private String engine;
    private Set<String> targets;
    private boolean targetAllMembers = false;
    private Map<String, Object> bindings;

    public ExecuteScriptRequest() {
    }

    public ExecuteScriptRequest(String script, String engine,
                                boolean targetAllMembers, Map<String, Object> bindings) {
        this.script = script;
        this.engine = engine;
        this.targets = new HashSet<String>(0);
        this.targetAllMembers = targetAllMembers;
        this.bindings = bindings;
    }

    public ExecuteScriptRequest(String script, String engine,
                                Set<String> targets, Map<String, Object> bindings) {
        this.script = script;
        this.targets = targets;
        this.engine = engine;
        this.targetAllMembers = false;
        this.bindings = bindings;
    }

    @Override
    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_EXECUTE_SCRIPT;
    }


    @Override
    public void writeResponse(ManagementCenterService mcs, JsonObject root) throws Exception {
        final JsonObject jsonResult = new JsonObject();
        ArrayList results;
        if (targetAllMembers) {
            final Set<Member> members = mcs.getHazelcastInstance().getCluster().getMembers();
            final ArrayList list = new ArrayList(members.size());
            for (Member member : members) {
                list.add(mcs.callOnMember(member, new ScriptExecutorOperation(engine, script, bindings)));
            }
            results = list;
        } else {
            final ArrayList list = new ArrayList(targets.size());
            for (String address : targets) {
                final AddressUtil.AddressHolder addressHolder = AddressUtil.getAddressHolder(address);
                final Address targetAddress = new Address(addressHolder.getAddress(), addressHolder.getPort());
                list.add(mcs.callOnAddress(targetAddress, new ScriptExecutorOperation(engine, script, bindings)));
            }
            results = list;
        }

        StringBuffer sb = new StringBuffer();
        for (Object result : results) {
            if (result instanceof String) {
                sb.append(result);
            } else if (result instanceof List) {
                final List list = (List) result;
                for (Object o : list) {
                    sb.append(o).append("\n");
                }
            } else if (result instanceof Map) {
                final Map map = (Map) result;
                for (Object o : map.entrySet()) {
                    final Map.Entry entry = (Map.Entry) o;
                    sb.append(entry.getKey()).append("->").append(entry.getValue()).append("\n");
                }
            } else if (result == null) {
                sb.append("error");
            }
            sb.append("\n");
        }
        jsonResult.add("scriptResult", sb.toString());
        root.add("result", jsonResult);
    }

    @Override
    public Object readResponse(JsonObject json) throws IOException {
        return getString(json, "scriptResult", "Error while reading response " + ExecuteScriptRequest.class.getName());
    }

    @Override
    public JsonObject toJson() {
        final JsonObject root = new JsonObject();
        root.add("script", script);
        root.add("engine", engine);
        JsonArray jsonTargets = new JsonArray();
        for (String target : targets) {
            jsonTargets.add(target);
        }
        root.add("targets", jsonTargets);
        root.add("targetAllMembers", targetAllMembers);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        script = getString(json, "script", "");
        engine = getString(json, "engine", "");
        targets = new HashSet<String>();
        for (JsonValue target : getArray(json, "targets", new JsonArray())) {
            targets.add(target.asString());
        }
        targetAllMembers = getBoolean(json, "targetAllMembers", false);
        bindings = new HashMap<String, Object>();
    }
}
