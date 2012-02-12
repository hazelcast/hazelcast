/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.web.tomcat;

import com.hazelcast.core.IMap;
import org.apache.catalina.Lifecycle;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.LifecycleListener;
import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;
import org.apache.catalina.util.LifecycleSupport;
import org.apache.catalina.valves.ValveBase;

import javax.servlet.ServletException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author ali
 */

public class HazelcastValve extends ValveBase implements Lifecycle {

    private final AtomicLong lastRequestId = new AtomicLong(0);
    private final LifecycleSupport lifecycle = new LifecycleSupport(this);

    public void invoke(Request request, Response response) throws IOException, ServletException {
        final long requestId = lastRequestId.incrementAndGet();
        LocalRequestId.set(requestId);
        try {
            final IMap<String, HazelcastAttribute> sessionAttrMap = HazelcastClusterSupport.get().getAttributesMap();
            getNext().invoke(request, response);
            final HazelcastSessionFacade session = (HazelcastSessionFacade) request.getSession(false);
            if (session != null) {
                final List<HazelcastAttribute> touchedAttributes = session.getTouchedAttributes(requestId);
                for (HazelcastAttribute attribute : touchedAttributes) {
                    sessionAttrMap.put(attribute.getKey(), attribute);
                }
            }
        } finally {
            LocalRequestId.reset();
        }
    }

    public void start() throws LifecycleException {
        lifecycle.fireLifecycleEvent(START_EVENT, null);
        HazelcastClusterSupport.get().start();
    }

    public void stop() throws LifecycleException {
        lifecycle.fireLifecycleEvent(STOP_EVENT, null);
        HazelcastClusterSupport.get().stop();
    }

    public void addLifecycleListener(LifecycleListener l) {
        lifecycle.addLifecycleListener(l);
    }

    public LifecycleListener[] findLifecycleListeners() {
        return lifecycle.findLifecycleListeners();
    }

    public void removeLifecycleListener(LifecycleListener l) {
        lifecycle.removeLifecycleListener(l);
    }
}
