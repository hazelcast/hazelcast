/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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

package com.hazelcast.monitor.client;

import java.util.HashMap;
import java.util.Map;

public class OnValueChangeHandler {

    Map<String, PageBuilder> mapPageBuilders = new HashMap<String, PageBuilder>();
    final private ServicesFactory servicesFactory;
    final private HazelcastMonitor hazelcastMonitor;

    public OnValueChangeHandler(ServicesFactory servicesFactory, HazelcastMonitor hazelcastMonitor) {
        this.servicesFactory = servicesFactory;
        this.hazelcastMonitor = hazelcastMonitor;
        initPageBuilders();
    }

    public void handle(String token) {
        Map<String, String> map = parseParamString(token);
        String name = map.get("name");
        int clusterId = Integer.valueOf(map.get("clusterId"));
        String type = map.get("type");
        hazelcastMonitor.closeClusterAddPanel();
        deRegisterAllActivePanels();
        ClusterWidgets clusterWidgets = hazelcastMonitor.getMapClusterWidgets().get(clusterId);
        PageBuilder pageBuilder = mapPageBuilders.get(type);
        pageBuilder.buildPage(clusterWidgets, name, new RegisterEventCallBack(hazelcastMonitor), servicesFactory);
    }

    private void deRegisterAllActivePanels() {
        for (ClusterWidgets cw : hazelcastMonitor.getMapClusterWidgets().values()) {
            cw.deRegisterAll();
        }
    }

    private HashMap<String, String> parseParamString(String string) {
        String[] ray = string.substring(0, string.length()).split("&");
        HashMap<String, String> map = new HashMap<String, String>();
        for (int i = 0; i < ray.length; i++) {
            String[] substrRay = ray[i].split("=");
            map.put(substrRay[0], substrRay[1]);
        }
        return map;
    }

    private void initPageBuilders() {
        mapPageBuilders.put("MEMBER", new MembersPageBuilder());
        mapPageBuilders.put("PARTITIONS", new PartitionsPageBuilder());
        mapPageBuilders.put(InstanceType.MAP.toString(), new MapPageBuilder());
    }
}
