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

import com.google.gwt.core.client.GWT;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.*;
import com.hazelcast.monitor.client.event.ChangeEvent;
import com.hazelcast.monitor.client.event.ChangeEventType;
import com.hazelcast.monitor.client.event.MapStatistics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class MapStatisticsPanel implements MonitoringPanel {
    DisclosurePanel dsp;

    private String mapName;
    private AsyncCallback<ChangeEvent> callBack;

    private final HazelcastServiceAsync hazelcastService = GWT
            .create(HazelcastService.class);

    public MapStatisticsPanel(String name, AsyncCallback<ChangeEvent> callBack) {
        this.mapName = name;
        this.callBack = callBack;
        dsp = initPanel();

    }

    public Widget getDsp() {
        return dsp;
    }


    public boolean register(ClusterWidgets clusterWidgets) {
        List<MonitoringPanel> list = clusterWidgets.getPanels().get(ChangeEventType.MAP_STATISTICS);
        if (list == null) {
            list = new ArrayList<MonitoringPanel>();
            clusterWidgets.getPanels().put(ChangeEventType.MAP_STATISTICS, list);
        }
        if (!list.contains(this)) {
            list.add(this);
        }
        hazelcastService.registerEvent(ChangeEventType.MAP_STATISTICS, clusterWidgets.clusterId, mapName, callBack);
        return true;
    }

    public boolean deRegister(ClusterWidgets clusterWidgets) {
        List<MonitoringPanel> list = clusterWidgets.getPanels().get(ChangeEventType.MAP_STATISTICS);
        if (list == null) {
            return false;
        }
        hazelcastService.deRegisterEvent(ChangeEventType.MAP_STATISTICS, clusterWidgets.clusterId, mapName, new AsyncCallback<Void>() {

            public void onFailure(Throwable caught) {
            }

            public void onSuccess(Void result) {
            }
        });
        return list.remove(this);
    }

    public void handle(ChangeEvent e) {
        MapStatistics event = (MapStatistics) e;
        int size = event.getSize();
        VerticalPanel vPanel = (VerticalPanel) dsp.getContent();
        Label label = (Label) vPanel.getWidget(0);
        label.setText("Size is: " + size);
        FlexTable table = (FlexTable) vPanel.getWidget(1);
        int row = 1;

        Collection<MapStatistics.LocalMapStatistics> collection =  event.getListOfLocalStats();
        System.out.println("Size of Collection is: "+collection.size());
        for (MapStatistics.LocalMapStatistics localMapStatistics: collection) {
            table.setText(row, 0, localMapStatistics.memberName);
            table.setText(row, 1, ""+localMapStatistics.ownedEntryCount);
            table.setText(row, 2, ""+localMapStatistics.ownedEntryMemoryCost);
            table.setText(row, 3, ""+localMapStatistics.backupEntryCount);
            table.setText(row, 4, ""+localMapStatistics.backupEntryMemoryCost);
            table.setText(row, 5, ""+localMapStatistics.markedAsRemovedEntryCount);
            table.setText(row, 6, ""+localMapStatistics.markedAsRemovedMemoryCost);
            table.setText(row, 7, ""+localMapStatistics.lockWaitCount);
            table.setText(row, 8, ""+localMapStatistics.lockedEntryCount);
            table.setText(row, 9, ""+localMapStatistics.hits);
            table.setText(row, 10, ""+localMapStatistics.lastAccessTime);
            table.setText(row, 11, ""+localMapStatistics.lastEvictionTime);
            table.setText(row, 12, ""+localMapStatistics.lastUpdateTime);
            table.setText(row, 13, ""+localMapStatistics.creationTime);
            row ++;
        }

        Image image = (Image) vPanel.getWidget(2);
        image.setUrl("/ChartGenerator?name=" + mapName + "&dummy = " + Math.random() * 10);
    }

    private DisclosurePanel initPanel() {
        final DisclosurePanel disclosurePanel = new DisclosurePanel("Map Statistics");
        VerticalPanel vPanel = new VerticalPanel();
        vPanel.add(new Label());
        FlexTable table = new FlexTable();
        table.setText(0, 0, "Members");
        table.setText(0, 1, "Owned Entry Count");
        table.setText(0, 2, "Owned Entry Memory Cost");
        table.setText(0, 3, "Backup Entry Count");
        table.setText(0, 4, "Backup Entry Memory Cost");
        table.setText(0, 5, "Marked as Remove Entry Count");
        table.setText(0, 6, "Marked as Remove Memory Cost");
        table.setText(0, 7, "Total Number of Waits on Lock");
        table.setText(0, 8, "Locked Entry Count");
        table.setText(0, 9, "Hits");
        table.setText(0, 10, "Last Access Time");
        table.setText(0, 11, "Last Eviction Time");
        table.setText(0, 12, "Last Update Time");
        table.setText(0, 13, "Creation Time");
        vPanel.add(table);

        vPanel.add(new Image());
        disclosurePanel.add(vPanel);
        disclosurePanel.setOpen(true);
        return disclosurePanel;
    }
}
