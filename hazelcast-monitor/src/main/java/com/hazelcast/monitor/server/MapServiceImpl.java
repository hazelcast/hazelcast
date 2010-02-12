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
package com.hazelcast.monitor.server;

import com.google.gwt.user.server.rpc.RemoteServiceServlet;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.monitor.client.MapService;

import java.util.Map;

import static com.hazelcast.monitor.server.HazelcastServiceImpl.getSessionObject;

public class MapServiceImpl extends RemoteServiceServlet implements MapService{
    public String get(int clusterId, String name, String key) {
        final SessionObject sessionObject = getSessionObject(this.getThreadLocalRequest().getSession());
        HazelcastInstance hz = sessionObject.mapOfHz.get(clusterId);
        Map map = hz.getMap(name);
        Object value = map.get(key);
        return (value==null)?null:value.toString();
    }
}
