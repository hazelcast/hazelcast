/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.eclipsesource.json.JsonObject;
import com.hazelcast.core.Member;
import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.management.operation.ChangeWanStateOperation;
import com.hazelcast.nio.Address;

import java.io.IOException;
import java.util.Set;

import static com.hazelcast.util.JsonUtil.getBoolean;
import static com.hazelcast.util.JsonUtil.getString;

/**
 * Request coming from Management Center for {@link ChangeWanStateOperation}
 */
public class ChangeWanStateRequest implements ConsoleRequest {

    private String memberAddress;
    private String schemeName;
    private String publisherName;
    private boolean start;

    public ChangeWanStateRequest() {
    }

    public ChangeWanStateRequest(String member, String schemeName, String publisherName, boolean start) {
        this.memberAddress = member;
        this.schemeName = schemeName;
        this.publisherName = publisherName;
        this.start = start;
    }

    @Override
    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_WAN_PUBLISHER;
    }

    @Override
    public Object readResponse(JsonObject in) throws IOException {
        return "success";
    }

    @Override
    public void writeResponse(ManagementCenterService mcs, JsonObject out) throws Exception {
        final JsonObject result = new JsonObject();
        result.add("start", start);
        ChangeWanStateOperation changeWanStateOperation =
                new ChangeWanStateOperation(schemeName, publisherName, start);

        String[] hostAndPort = memberAddress.split(":");
        Address address = new Address(hostAndPort[0], Integer.parseInt(hostAndPort[1]));

        final Set<Member> members = mcs.getHazelcastInstance().getCluster().getMembers();
        for (Member member : members) {
            if (member.getAddress().equals(address)) {
                mcs.callOnMember(member, changeWanStateOperation);
                break;
            }
        }
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("member", memberAddress);
        root.add("schemeName", schemeName);
        root.add("publisherName", publisherName);
        root.add("start", start);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        memberAddress = getString(json, "member");
        schemeName = getString(json, "schemeName");
        publisherName = getString(json, "publisherName");
        start = getBoolean(json, "start");
    }
}
