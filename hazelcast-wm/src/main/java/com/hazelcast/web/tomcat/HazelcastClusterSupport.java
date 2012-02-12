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

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class HazelcastClusterSupport {

    private static final String SESSION_ATTR_MAP = "__hz_ses_attrs";
    private static final HazelcastClusterSupport INSTANCE = new HazelcastClusterSupport();

    public static HazelcastClusterSupport get() {
        return INSTANCE;
    }

    private final HazelcastInstance hazelcast;
    private IMap<String, HazelcastAttribute> sessionMap;

    private HazelcastClusterSupport() {
        super();
        hazelcast = Hazelcast.newHazelcastInstance(null);
    }

    public IMap<String, HazelcastAttribute> getAttributesMap() {
        return sessionMap;
    }

    public void start() {
        sessionMap = hazelcast.getMap(SESSION_ATTR_MAP);
    }

    public void stop() {
        hazelcast.getLifecycleService().shutdown();
    }
}
