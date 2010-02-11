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

import com.google.gwt.i18n.client.DateTimeFormat;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.AbsolutePanel;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.hazelcast.monitor.client.event.ChangeEvent;
import com.hazelcast.monitor.client.event.MapStatistics;

import java.util.Collection;
import java.util.Date;

import static com.hazelcast.monitor.client.PanelUtils.createFormattedFlexTable;
import static com.hazelcast.monitor.client.PanelUtils.formatEvenRows;
import static com.hazelcast.monitor.client.PanelUtils.removeUnusedRows;

public class MapTimesPanel extends MapPanel implements MonitoringPanel {

    public MapTimesPanel(String name, AsyncCallback<ChangeEvent> callBack) {
        super(name, callBack, "Times for Map: " + name);
    }

    @Override
    protected FlexTable createTable() {
        FlexTable table = createFormattedFlexTable();

//        table.addStyleName("table");
        table.setWidget(0, 0, new LabelWithToolTip("Members", "Members of the Cluster"));
        table.setWidget(0, 1, new LabelWithToolTip("Last Access Time", "Last Access Time"));
        table.setWidget(0, 2, new LabelWithToolTip("Last Eviction Time", "Last Eviction Time"));
        table.setWidget(0, 3, new LabelWithToolTip("Last Update Time", "Last Update Time"));
        table.setWidget(0, 4, new LabelWithToolTip("Map Creation Time", "Creation Time of the Map"));
//        table.getRowFormatter().addStyleName(0, "mapstatsHeader");
//        table.addStyleName("mapstats");
        return table;
    }

    public void handle(ChangeEvent e) {
        MapStatistics event = (MapStatistics) e;
        VerticalPanel vPanel = (VerticalPanel) disclosurePanel.getContent();
        AbsolutePanel absTablePanel = (AbsolutePanel) vPanel.getWidget(1);
        FlexTable table = (FlexTable) absTablePanel.getWidget(0);
        int row = 1;
        Collection<MapStatistics.LocalMapStatistics> collection = event.getListOfLocalStats();
        DateTimeFormat ttipFormat = DateTimeFormat.getFormat("yyyy.MM.dd HH:mm:ss");
        DateTimeFormat displayFormat = DateTimeFormat.getFormat("HH:mm:ss");
        for (MapStatistics.LocalMapStatistics localMapStatistics : collection) {
            table.setWidget(row, 0, clusterWidgets.getInstanceLink(null, localMapStatistics.memberName));
            addDateToTable(table, row, 1, new Date(localMapStatistics.lastAccessTime), ttipFormat, displayFormat);
            addDateToTable(table, row, 2, new Date(localMapStatistics.lastEvictionTime), ttipFormat, displayFormat);
            addDateToTable(table, row, 3, new Date(localMapStatistics.lastUpdateTime), ttipFormat, displayFormat);
            addDateToTable(table, row, 4, new Date(localMapStatistics.creationTime), ttipFormat, displayFormat);
            table.getColumnFormatter().addStyleName(0, "mapstatsStringColumn");
            table.getCellFormatter().addStyleName(row, 1, "mapstatsStringColumn");
            table.getCellFormatter().addStyleName(row, 2, "mapstatsStringColumn");
            table.getCellFormatter().addStyleName(row, 3, "mapstatsStringColumn");
            table.getCellFormatter().addStyleName(row, 4, "mapstatsStringColumn");
            formatEvenRows(row, table);
            row++;
        }
        removeUnusedRows(row, table);
        
    }

    private void addDateToTable(FlexTable table, int row, int col, Date date, DateTimeFormat ttipFormat, DateTimeFormat displayFormat) {
        table.setWidget(row, col, new MapPanel.LabelWithToolTip(displayFormat.format(date), ttipFormat.format(date)));
    }
}
