package com.hazelcast.wm.test;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

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

        } else if (req.getRequestURI().endsWith("invalidate")) {
            session.invalidate();
            resp.getWriter().write("true");

        } else if (req.getRequestURI().endsWith("update")) {
            session.setAttribute("key", "value-updated");
            resp.getWriter().write("true");

        } else if (req.getRequestURI().endsWith("reload")) {
            session.invalidate();
            session = req.getSession();
            session.setAttribute("first-key", "first-value");
            session.setAttribute("second-key", "second-value");
            resp.getWriter().write("true");
        }
    }
}
