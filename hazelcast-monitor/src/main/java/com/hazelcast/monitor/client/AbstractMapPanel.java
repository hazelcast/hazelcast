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
package com.hazelcast.monitor.client;

import com.hazelcast.monitor.client.event.ChangeEventType;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractMapPanel implements MonitoringPanel{
    /**
     * Registers the panel in ClusterWidgets;
     * If it existed before, returns false. Otherwise returns true.
     *
     * @param clusterWidgets
     * @param changeEventType
     * @return
     */
    protected boolean register(ClusterWidgets clusterWidgets, ChangeEventType changeEventType){
        List<MonitoringPanel> list = clusterWidgets.getPanels().get(changeEventType);
        if (list == null) {
            list = new ArrayList<MonitoringPanel>();
            clusterWidgets.getPanels().put(changeEventType, list);
        }
        if (!list.contains(this)) {
            list.add(this);
            return true;
        }
        else{
            return false;
        }

    }

    /** De registers the panel from clusterWidgets. Returns true of there is no registered panel
     * with same ChangeEventType.
     *
     * @param clusterWidgets
     * @param changeEventType
     * @return
     */
    protected boolean deRegister(ClusterWidgets clusterWidgets, ChangeEventType changeEventType){
        List<MonitoringPanel> list = clusterWidgets.getPanels().get(changeEventType);
        if (list != null) {
            list.remove(this);
        }
        return list.isEmpty();
    }
}
