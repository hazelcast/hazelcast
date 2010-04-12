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

import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.FlexTable;
import com.hazelcast.monitor.client.event.ChangeEvent;
import com.hazelcast.monitor.client.event.ChangeEventType;
import com.hazelcast.monitor.client.event.LocalInstanceStatistics;
import com.hazelcast.monitor.client.event.MapStatistics;

public abstract class MapStatsPanel extends InstanceStatsPanel implements MonitoringPanel {

    public MapStatsPanel(String name, AsyncCallback<ChangeEvent> callBack, String panelLabel, HazelcastServiceAsync hazelcastServiceAsync) {
        super(name, callBack, panelLabel, hazelcastServiceAsync, ChangeEventType.MAP_STATISTICS);
    }

    protected void handleRow(FlexTable table, int row, LocalInstanceStatistics localInstanceStatistics) {
        handleRow(table, row, (MapStatistics.LocalMapStatistics)localInstanceStatistics);
    }

    protected abstract void handleRow(FlexTable table, int row, MapStatistics.LocalMapStatistics localMapStatistics) ;
}
