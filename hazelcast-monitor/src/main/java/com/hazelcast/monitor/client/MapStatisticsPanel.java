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
import com.google.gwt.user.client.ui.*;
import com.hazelcast.monitor.client.event.ChangeEvent;
import com.hazelcast.monitor.client.event.MapStatistics;

import java.util.Collection;

public class MapStatisticsPanel extends MapPanel implements MonitoringPanel {

    public MapStatisticsPanel(String name, AsyncCallback<ChangeEvent> callBack) {
        super(name, callBack, "Statistics for Map: " + name);
    }

    public void handle(ChangeEvent e) {
        MapStatistics event = (MapStatistics) e;
        int size = event.getSize();
        VerticalPanel vPanel = (VerticalPanel) disclosurePanel.getContent();
//        Label label = (Label) vPanel.getWidget(0);
//        label.setText("Map Size: " + size);
        AbsolutePanel absTablePanel = (AbsolutePanel) vPanel.getWidget(1);
        FlexTable table = (FlexTable) absTablePanel.getWidget(0);
        int row = 1;
        Collection<MapStatistics.LocalMapStatistics> collection = event.getListOfLocalStats();
        for (MapStatistics.LocalMapStatistics localMapStatistics : collection) {
            table.setText(row, 0, localMapStatistics.memberName);
            table.setText(row, 1, "" + localMapStatistics.ownedEntryCount);
            table.setText(row, 2, "" + formatMemorySize(localMapStatistics.ownedEntryMemoryCost));
            table.setText(row, 3, "" + localMapStatistics.backupEntryCount);
            table.setText(row, 4, "" + formatMemorySize(localMapStatistics.backupEntryMemoryCost));
            table.setText(row, 5, "" + localMapStatistics.markedAsRemovedEntryCount);
            table.setText(row, 6, "" + formatMemorySize(localMapStatistics.markedAsRemovedMemoryCost));
            table.setText(row, 7, "" + localMapStatistics.lockedEntryCount);
            table.setText(row, 8, "" + localMapStatistics.lockWaitCount);
            table.setText(row, 9, "" + localMapStatistics.hits);
            table.getColumnFormatter().addStyleName(0, "mapstatsStringColumn");
            table.getCellFormatter().addStyleName(row, 1, "mapstatsNumericColumn");
            table.getCellFormatter().addStyleName(row, 3, "mapstatsNumericColumn");
            table.getCellFormatter().addStyleName(row, 5, "mapstatsNumericColumn");
            table.getCellFormatter().addStyleName(row, 7, "mapstatsNumericColumn");
            table.getCellFormatter().addStyleName(row, 8, "mapstatsNumericColumn");
            table.getCellFormatter().addStyleName(row, 9, "mapstatsNumericColumn");
            table.getCellFormatter().addStyleName(row, 2, "mapstatsStringColumn");
            table.getCellFormatter().addStyleName(row, 4, "mapstatsStringColumn");
            table.getCellFormatter().addStyleName(row, 6, "mapstatsStringColumn");
            if (row % 2 == 0) {
                table.getRowFormatter().addStyleName(row, "mapstatsEvenRow");
            }
            row++;
        }
        while (table.getRowCount() > row) {
            table.removeRow(row);
        }
//        if (vPanel.getWidgetCount() < 3) {
//            AbsolutePanel absImagePanel = new AbsolutePanel();
//            absImagePanel.addStyleName("img-shadow");
//            absImagePanel.add(new Image());
//            vPanel.add(absImagePanel);
//        }
//        AbsolutePanel absolutePanel = (AbsolutePanel) vPanel.getWidget(2);
//        Image image = (Image) absolutePanel.getWidget(0);
//        image.setUrl("ChartGenerator?name=" + mapName + "&dummy = " + Math.random() * 10);
    }

    private String formatMemorySize(long size) {
        int gb = 1024 * 1024 * 1024;
        int mb = 1024 * 1024;
        int kb = 1024;
        double result;
        if ((result = (double) size / gb) >= 1) {
            return toPrecision(result) + " GB";
        } else if ((result = (double) size / mb) >= 1) {
            return Math.round(result) + " MB";
        } else if ((result = (double) size / kb) >= 1) {
            return Math.round(result) + " KB";
        } else {
            return size + " Bytes";
        }
    }

    private static String toPrecision(double dbl) {
        int ix = (int) (dbl * 100.0); // scale it
        double dbl2 = ((double) ix) / 100.0;
        return String.valueOf(dbl2);
    }

    @Override
    protected FlexTable createTable() {
        FlexTable table = new FlexTable();
        table.addStyleName("table");
        table.setWidget(0, 0, new LabelWithToolTip("Members", "Members of the Cluster"));
        table.setWidget(0, 1, new LabelWithToolTip("Entries", "Number of Entries"));
        table.setWidget(0, 2, new LabelWithToolTip("Entry Memory", "Memory Size of Entries"));
        table.setWidget(0, 3, new LabelWithToolTip("Backups", "Number of Backups"));
        table.setWidget(0, 4, new LabelWithToolTip("Backup Memory", "Memory Size of Backups"));
        table.setWidget(0, 5, new LabelWithToolTip("Marked as Remove", "Entries Marked as Remove"));
        table.setWidget(0, 6, new LabelWithToolTip("Memory", "Memory Size of the Entries Marked as Remove"));
        table.setWidget(0, 7, new LabelWithToolTip("Locks", "Number of Locked Entries"));
        table.setWidget(0, 8, new LabelWithToolTip("Threads Waiting on Lock", "Total Number of Threads Waiting on Locked Entries"));
        table.setWidget(0, 9, new LabelWithToolTip("Total Hits", "Number of Hits"));
        table.getRowFormatter().addStyleName(0, "mapstatsHeader");
        table.addStyleName("mapstats");
        return table;
    }
}
