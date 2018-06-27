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

import com.eclipsesource.json.JsonObject;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;

public class MancenterServlet extends HttpServlet {

    private volatile TimedMemberState memberState = null;
    private volatile String clusterName = "";

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        if (req.getPathInfo().contains("memberStateCheck")) {
            if (memberState != null) {
                resp.getWriter().write(memberState.toJson().toString());
            } else {
                resp.getWriter().write("");
            }
        } else if (req.getPathInfo().contains("getTask.do")) {
            clusterName = req.getParameter("cluster");
            resp.getWriter().write("{}");
        } else if (req.getPathInfo().contains("getClusterName")) {
            resp.getWriter().write(clusterName);
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        BufferedReader br = req.getReader();
        StringBuilder sb = new StringBuilder();
        String str;
        while ((str = br.readLine()) != null) {
            sb.append(str);
        }
        final String json = sb.toString();
        final JsonObject object = JsonObject.readFrom(json);
        process(object);
    }

    public void process(JsonObject json) {
        ManagementCenterIdentifier identifier = new ManagementCenterIdentifier();
        identifier.fromJson(json.get("identifier").asObject());
        memberState = new TimedMemberState();
        memberState.fromJson(json.get("timedMemberState").asObject());
    }
}
