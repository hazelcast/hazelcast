/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.web;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionListener;

public class SessionListener implements HttpSessionListener {

    private static final ILogger LOGGER = Logger.getLogger(SessionListener.class);

    public void sessionCreated(HttpSessionEvent event) {
    }

    public void sessionDestroyed(HttpSessionEvent event) {
        HttpSession session = event.getSession();

        ServletContext servletContext = session.getServletContext();
        WebFilter webFilter = (WebFilter) servletContext.getAttribute(WebFilter.class.getName());
        if (webFilter == null) {
            LOGGER.warning("The " + WebFilter.class.getName() + " could not be found. " + getClass().getName() +
                    " should be paired with a " + WebFilter.class.getName() + ".");
        } else {
            webFilter.destroyOriginalSession(session);
        }
    }
}
