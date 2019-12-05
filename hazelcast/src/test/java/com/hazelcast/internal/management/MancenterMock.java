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

package com.hazelcast.internal.management;

import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.management.dto.ClientBwListDTO;
import com.hazelcast.internal.management.dto.ClientBwListEntryDTO;
import fi.iki.elonen.NanoHTTPD;
import fi.iki.elonen.NanoHTTPD.Response.Status;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.internal.management.dto.ClientBwListDTO.Mode;
import static com.hazelcast.internal.management.dto.ClientBwListEntryDTO.Type;

public class MancenterMock extends NanoHTTPD {

    private volatile ClientBwListDTO clientBwList;

    public MancenterMock(int port) throws IOException {
        super(port);
        resetClientBwList();
        start();
    }

    private volatile TimedMemberState memberState = null;
    private volatile String clusterName = "";

    @Override
    public Response serve(IHTTPSession session) {
        String path = session.getUri();
        Map<String, String> files = new HashMap<String, String>();
        Method method = session.getMethod();
        if (Method.PUT.equals(method) || Method.POST.equals(method)) {
            try {
                session.parseBody(files);
            } catch (IOException ioe) {
                return newFixedLengthResponse(Status.INTERNAL_ERROR, MIME_PLAINTEXT, ioe.getMessage());
            } catch (ResponseException re) {
                return newFixedLengthResponse(re.getStatus(), MIME_PLAINTEXT, re.getMessage());
            }
        }
        if (path.contains("memberStateCheck")) {
            if (memberState != null) {
                return newFixedLengthResponse(Status.OK, "application/json", memberState.toJson().toString());
            }
        } else if (path.contains("getClusterName")) {
            return newFixedLengthResponse(Status.OK, MIME_PLAINTEXT, clusterName);
        } else if (path.contains("getTask.do")) {
            clusterName = session.getParameters().get("cluster").get(0);
            return newFixedLengthResponse(Status.OK, "application/json", "{}");
        } else if (path.contains("collector.do")) {
            final JsonObject object = Json.parse(files.get("postData")).asObject();
            process(object);
            return newFixedLengthResponse(Status.OK, "application/json", getConfigString());
        }
        return newFixedLengthResponse("");
    }

    public void process(JsonObject json) {
        memberState = new TimedMemberState();
        memberState.fromJson(json.get("timedMemberState").asObject());
    }

    public void enableClientWhitelist(String... ips) {
        List<ClientBwListEntryDTO> entries = new ArrayList<ClientBwListEntryDTO>();
        if (ips != null) {
            for (String ip : ips) {
                entries.add(new ClientBwListEntryDTO(Type.IP_ADDRESS, ip));
            }
        }
        clientBwList = new ClientBwListDTO(Mode.WHITELIST, entries);
    }

    public void resetClientBwList() {
        clientBwList = new ClientBwListDTO(Mode.DISABLED, new ArrayList<ClientBwListEntryDTO>());
    }

    private String getConfigString() {
        return "{\"clientBwList\":" + clientBwList.toJson().toString() + "}";
    }

}
