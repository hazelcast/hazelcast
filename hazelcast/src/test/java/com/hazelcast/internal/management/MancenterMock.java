/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;

import fi.iki.elonen.NanoHTTPD;
import fi.iki.elonen.NanoHTTPD.Response.Status;

public class MancenterMock extends NanoHTTPD {

    public MancenterMock(int port) throws IOException {
        super(port);
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
        }
        return newFixedLengthResponse("");
    }

    public void process(JsonObject json) {
        memberState = new TimedMemberState();
        memberState.fromJson(json.get("timedMemberState").asObject());
    }
}
