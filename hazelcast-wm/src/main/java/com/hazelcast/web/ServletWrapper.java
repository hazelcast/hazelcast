/* 
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.web;

import com.hazelcast.web.WebFilter.RequestWrapper;

import javax.servlet.*;
import java.io.IOException;
import java.util.Enumeration;

public class ServletWrapper extends ServletBase {
    GenericServlet base = null;

    public static final String HAZELCAST_BASE_SERVLET_CLASS = "*hazelcast-base-servlet-class";

    private void initBase(ServletConfig servletConfig) {
        if (base == null) {
            String baseClassName = servletConfig.getInitParameter(HAZELCAST_BASE_SERVLET_CLASS);
            try {
                base = (GenericServlet) Class.forName(baseClassName).newInstance();
                debug("Will use base servlet class " + base);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void init(ServletConfig servletConfig) throws ServletException {
        super.init(servletConfig);
        initBase(servletConfig);
        base.init(getServletConfig());
    }

    @Override
    public void destroy() {
        base.destroy();
    }

    @Override
    public String getInitParameter(String name) {
        return base.getInitParameter(name);
    }

    @Override
    public Enumeration getInitParameterNames() {
        return base.getInitParameterNames();
    }

    @Override
    public void service(ServletRequest req, ServletResponse res) throws IOException,
            ServletException {
        if (!(req instanceof RequestWrapper)) {
            ServletRequest reqHazel = (ServletRequest) req
                    .getAttribute(WebFilter.HAZELCAST_REQUEST);
            if (reqHazel != null)
                req = reqHazel;
        }
        base.service(req, res);
    }

    public String getServiceName() {
        return base.getServletName();
    }

    @Override
    public String getServletInfo() {
        return base.getServletInfo();
    }
}
