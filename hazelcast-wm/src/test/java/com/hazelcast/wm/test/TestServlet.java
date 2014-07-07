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

package com.hazelcast.wm.test;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class TestServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        HttpSession session = req.getSession();
        if (req.getRequestURI().endsWith("write")) {
            session.setAttribute("key", "value");
            resp.getWriter().write("true");

        } else if (req.getRequestURI().endsWith("read")) {
            Object value = session.getAttribute("key");
            resp.getWriter().write(value == null ? "null" : value.toString());

        } else if (req.getRequestURI().endsWith("remove")) {
            session.removeAttribute("key");
            resp.getWriter().write("true");

        } else if (req.getRequestURI().endsWith("remove_set_null")) {
            session.setAttribute("key", null);
            resp.getWriter().write("true");

        } else if (req.getRequestURI().endsWith("invalidate")) {
            session.invalidate();
            resp.getWriter().write("true");

        } else if (req.getRequestURI().endsWith("update")) {
            session.setAttribute("key", "value-updated");
            resp.getWriter().write("true");

        } else if (req.getRequestURI().endsWith("update-and-read-same-request")) {
            session.setAttribute("key", "value-updated");
            Object value = session.getAttribute("key");
            resp.getWriter().write(value == null ? "null" : value.toString());
        } else if (req.getRequestURI().endsWith("names")) {
            List<String> names = Collections.list(session.getAttributeNames());
            String nameList = names.toString();
            //return comma seperated list of attribute names
            resp.getWriter().write(nameList.substring(1, nameList.length() - 1).replace(", ", ","));

        } else if (req.getRequestURI().endsWith("reload")) {
            session.invalidate();
            session = req.getSession();
            session.setAttribute("first-key", "first-value");
            session.setAttribute("second-key", "second-value");
            resp.getWriter().write("true");
        }
    }
}
