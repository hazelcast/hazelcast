package com.hazelcast.internal.management;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.monitor.TimedMemberState;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;

public class MancenterServlet extends HttpServlet {

    private volatile TimedMemberState memberState = null;

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        if(req.getPathInfo().contains("memberStateCheck")) {
            if(memberState != null) {
                resp.getWriter().write(memberState.toJson().toString());
            } else {
                resp.getWriter().write("");
            }
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
