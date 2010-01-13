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
import com.google.gwt.event.dom.client.MouseOutEvent;
import com.google.gwt.event.dom.client.MouseOutHandler;
import com.google.gwt.event.dom.client.MouseOverEvent;
import com.google.gwt.event.dom.client.MouseOverHandler;
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
        label.setText("Current Map Size: " + size);
        AbsolutePanel absTablePanel = (AbsolutePanel)vPanel.getWidget(1);
        FlexTable table = (FlexTable) absTablePanel.getWidget(0);
        int row = 1;
        Collection<MapStatistics.LocalMapStatistics> collection = event.getListOfLocalStats();
        DateTimeFormat ttipFormat = DateTimeFormat.getFormat("yyyy.MM.dd HH:mm:ss");
        DateTimeFormat displayFormat = DateTimeFormat.getFormat("HH:mm:ss");
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
            addDateToTable(table, row, 10, new Date(localMapStatistics.lastAccessTime), ttipFormat, displayFormat);
            addDateToTable(table, row, 11, new Date(localMapStatistics.lastEvictionTime), ttipFormat, displayFormat);
            addDateToTable(table, row, 12, new Date(localMapStatistics.lastUpdateTime), ttipFormat, displayFormat);
            addDateToTable(table, row, 13, new Date(localMapStatistics.creationTime), ttipFormat, displayFormat);
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
            table.getCellFormatter().addStyleName(row, 10, "mapstatsStringColumn");
            table.getCellFormatter().addStyleName(row, 11, "mapstatsStringColumn");
            table.getCellFormatter().addStyleName(row, 12, "mapstatsStringColumn");
            table.getCellFormatter().addStyleName(row, 13, "mapstatsStringColumn");
            if(row%2==0){
                table.getRowFormatter().addStyleName(row, "mapstatsEvenRow");    
            }
            row++;
        }
        while (table.getRowCount() > row) {
            table.removeRow(row);
        }

        AbsolutePanel absolutePanel = (AbsolutePanel) vPanel.getWidget(2);
        Image image = (Image) absolutePanel.getWidget(0);

        image.setUrl("/ChartGenerator?name=" + mapName + "&dummy = " + Math.random() * 10);
    }

    private void addDateToTable(FlexTable table, int row, int col, Date date, DateTimeFormat ttipFormat, DateTimeFormat displayFormat) {
        table.setWidget(row, col, new LabelWithToolTip( displayFormat.format(date), ttipFormat.format(date)));
    }

    public static String formatMemorySize(long size) {
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

    private DisclosurePanel initPanel() {
        final DisclosurePanel disclosurePanel = new DisclosurePanel("Statistics For the Map: "+ mapName);
        VerticalPanel vPanel = new VerticalPanel();
        vPanel.add(new Label());
        FlexTable table = new FlexTable();
        table.addStyleName("table");
        table.setWidget(0, 0, new LabelWithToolTip("Members", "Members of the Cluster"));
        table.setWidget(0, 1, new LabelWithToolTip("Entries", "Number of Entries"));
        table.setWidget(0, 2, new LabelWithToolTip("Memory", "Memory Size of Entries"));
        table.setWidget(0, 3, new LabelWithToolTip("Backups", "Number of Backups"));
        table.setWidget(0, 4, new LabelWithToolTip("Memory", "Memory Size of Backups"));
        table.setWidget(0, 5, new LabelWithToolTip("M. Remove", "Entries Marked as Remove"));
        table.setWidget(0, 6, new LabelWithToolTip("Memory", "Memory Size of the Entries Marked as Remove"));
        table.setWidget(0, 7, new LabelWithToolTip("Locks", "Number of Locked Entries"));
        table.setWidget(0, 8, new LabelWithToolTip("W. Lock", "Total Number of Threads Waiting on Locked Entries"));
        table.setWidget(0, 9, new LabelWithToolTip("Hits", "Number of Hits"));
        table.setWidget(0, 10, new LabelWithToolTip("Last Access", "Last Access Time"));
        table.setWidget(0, 11, new LabelWithToolTip("Last Eviction", "Last Eviction Time"));
        table.setWidget(0, 12, new LabelWithToolTip("Last Update", "Last Update Time"));
        table.setWidget(0, 13, new LabelWithToolTip("Creation Time", "Creation Time of the Map"));

        table.getRowFormatter().addStyleName(0, "mapstatsHeader");
        table.addStyleName("mapstats");

        AbsolutePanel absTablePanel = new AbsolutePanel();
        absTablePanel.addStyleName("img-shadow");
        absTablePanel.add(table);
        vPanel.add(absTablePanel);
        AbsolutePanel absImagePanel = new AbsolutePanel();
        absImagePanel.addStyleName("img-shadow");
        absImagePanel.add(new Image());
        vPanel.add(absImagePanel);
        disclosurePanel.add(vPanel);
        disclosurePanel.setOpen(true);
        return disclosurePanel;
    }

    private class LabelWithToolTip extends Label {
        private LabelWithToolTip(final String label, final String toolTip) {
            super(label);
            final Map<Integer, ToolTip> map = new HashMap();
            this.addMouseOverHandler(new MouseOverHandler() {
                public void onMouseOver(MouseOverEvent event) {
                    String tip = toolTip;
                    if(tip==null || tip.equals("")){
                        tip = label;
                    }
                    ToolTip ttip = new ToolTip(tip, event.getClientX(), event.getClientY());
                    map.put(1, ttip);
                }
            });
            this.addMouseOutHandler(new MouseOutHandler(){
                public void onMouseOut(MouseOutEvent event) {
                    ToolTip tip = map.remove(1);
                    if(tip!=null){
                        tip.hide();
                    }
                }
            });
        }
    }
}
