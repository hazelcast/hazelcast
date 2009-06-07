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

import javax.servlet.ServletContext;
import java.util.EventObject;

public class SnapshotEvent extends EventObject {

    private static final long serialVersionUID = -201086537182587371L;

    private int createdSessions;

    private int destroyedSessions;

    private long minResponseTime;

    private long maxResponseTime;

    private long aveResponseTime;

    private long numberOfRequests;

    public SnapshotEvent(ServletContext servletContext, int createdSessions, int destroyedSessions,
                         long minResponseTime, long maxResponseTime, long aveResponseTime, long numberOfRequests) {
        super(servletContext);
        this.createdSessions = createdSessions;
        this.destroyedSessions = destroyedSessions;
        this.minResponseTime = minResponseTime;
        this.maxResponseTime = maxResponseTime;
        this.aveResponseTime = aveResponseTime;
        this.numberOfRequests = numberOfRequests;
    }

    public SnapshotEvent(ServletContext servletContext) {
        super(servletContext);
    }

    public ServletContext getServletContext() {
        return (ServletContext) getSource();
    }

    public long getAveResponseTime() {
        return aveResponseTime;
    }

    public int getCreatedSessions() {
        return createdSessions;
    }

    public int getDestroyedSessions() {
        return destroyedSessions;
    }

    public long getMaxResponseTime() {
        return maxResponseTime;
    }

    public long getMinResponseTime() {
        return minResponseTime;
    }

    public long getNumberOfRequests() {
        return numberOfRequests;
    }

    @Override
    public String toString() {
        return "SnapshotEvent {" + getServletContext().getServletContextName()
                + "} createdSessions:" + createdSessions + ", destroyedSessions:"
                + destroyedSessions + ", minResponseTime:" + minResponseTime + ", maxResponseTime:"
                + maxResponseTime + ", aveResponseTime:" + aveResponseTime + ", numberOfRequest:"
                + numberOfRequests;
    }

}
