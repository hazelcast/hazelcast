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
import com.google.gwt.i18n.client.DateTimeFormat;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.*;
import com.hazelcast.monitor.client.event.ChangeEvent;
import com.hazelcast.monitor.client.event.ChangeEventType;
import com.hazelcast.monitor.client.event.MapStatistics;

import java.util.*;

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
        label.setText("Size:"+ size);
        FlexTable table = (FlexTable) vPanel.getWidget(1);
        int row = 1;
        Collection<MapStatistics.LocalMapStatistics> collection = event.getListOfLocalStats();
        DateTimeFormat dtf = DateTimeFormat.getFormat("yyyy.MM.dd HH:mm:ss");
        for (MapStatistics.LocalMapStatistics localMapStatistics : collection) {
            table.setText(row, 0, localMapStatistics.memberName);
            table.setText(row, 1, "" + localMapStatistics.ownedEntryCount);
            table.setText(row, 2, "" + formatMemorySize(localMapStatistics.ownedEntryMemoryCost));
            table.setText(row, 3, "" + localMapStatistics.backupEntryCount);
            table.setText(row, 4, "" + formatMemorySize(localMapStatistics.backupEntryMemoryCost));
            table.setText(row, 5, "" + localMapStatistics.markedAsRemovedEntryCount);
            table.setText(row, 6, "" + formatMemorySize(localMapStatistics.markedAsRemovedMemoryCost));
            table.setText(row, 7, "" + localMapStatistics.lockWaitCount);
            table.setText(row, 8, "" + localMapStatistics.lockedEntryCount);
            table.setText(row, 9, "" + localMapStatistics.hits);
            table.setText(row, 10, dtf.format(new Date(localMapStatistics.lastAccessTime)));
            table.setText(row, 11, dtf.format(new Date(localMapStatistics.lastEvictionTime)));
            table.setText(row, 12, dtf.format(new Date(localMapStatistics.lastUpdateTime)));
            table.setText(row, 13, dtf.format(new Date(localMapStatistics.creationTime)));
            row++;
        }
        while (table.getRowCount() > row) {
            table.removeRow(row);
        }
        Image image = (Image) vPanel.getWidget(2);
        image.setUrl("/ChartGenerator?name=" + mapName + "&dummy = " + Math.random() * 10);
    }

    public static String formatMemorySize(long size) {
        int gb = 1024 * 1024 * 1024;
        int mb = 1024 * 1024;
        int kb = 1024;
        
        double result;
        if ((result = (double) size / gb) >= 1) {
            return toPrecision(result) + " GB";
        } else if ((result = (double) size / mb) >= 1) {
            return toPrecision(result) + " MB";
        } else if ((result = (double) size / kb) >= 1) {
            return toPrecision(result) + " KB";
        } else {
            return size + " Bytes";
        }
    }
    private static String toPrecision(double dbl){
        int ix = (int)(dbl * 100.0); // scale it
        double dbl2 = ((double)ix)/100.0;
        return String.valueOf(dbl2);
    }

    private DisclosurePanel initPanel() {
        final DisclosurePanel disclosurePanel = new DisclosurePanel("Map Statistics");
        VerticalPanel vPanel = new VerticalPanel();
        vPanel.add(new Label());
        FlexTable table = new FlexTable();
        table.setText(0, 0, "Members");
        table.setText(0, 1, "# of Entries");
        table.setText(0, 2, "Memory Size");
        table.setText(0, 3, "# of Backups");
        table.setText(0, 4, "Memory Size");
        table.setText(0, 5, "Marked as Remove");
        table.setText(0, 6, "Memory Size");
        table.setText(0, 7, "# of Waits on Lock");
        table.setText(0, 8, "# of Locks");
        table.setText(0, 9, "Hits");
        table.setText(0, 10, "Last Access");
        table.setText(0, 11, "Last Eviction");
        table.setText(0, 12, "Last Update");
        table.setText(0, 13, "Creation Time");
        vPanel.add(table);
        vPanel.add(new Image());
        disclosurePanel.add(vPanel);
        disclosurePanel.setOpen(true);
        return disclosurePanel;
    }
}
