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
import com.hazelcast.client.NoMemberAvailableException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.monitor.client.MapEntry;
import com.hazelcast.monitor.client.MapService;
import com.hazelcast.monitor.client.exception.ClientDisconnectedException;

import static com.hazelcast.monitor.server.HazelcastServiceImpl.getSessionObject;

public class MapServiceImpl extends RemoteServiceServlet implements MapService {
    public MapEntry get(int clusterId, String name, String key) {
        try {
            final SessionObject sessionObject = getSessionObject(this.getThreadLocalRequest().getSession());
            HazelcastInstance hz = sessionObject.mapOfHz.get(clusterId);
            IMap map = hz.getMap(name);
            com.hazelcast.core.MapEntry mapEntry = map.getMapEntry(key);
            return convertToMonitorMapEntry(mapEntry);
        } catch (NoMemberAvailableException e) {
            throw new ClientDisconnectedException();
        }
    }

    private MapEntry convertToMonitorMapEntry(com.hazelcast.core.MapEntry mapEntry) {
        if (mapEntry == null) {
            return null;
        }
        MapEntry result = new MapEntry();
        result.setCost(mapEntry.getCost());
        result.setCreationTime(mapEntry.getCreationTime());
        result.setExpirationTime(mapEntry.getExpirationTime());
        result.setHits(mapEntry.getHits());
        result.setLastAccessTime(mapEntry.getLastAccessTime());
        result.setLastUpdateTime(mapEntry.getLastUpdateTime());
        result.setValid(mapEntry.isValid());
        result.setValue(mapEntry.getValue());
        result.setVersion(mapEntry.getVersion());
        return result;
    }
}

