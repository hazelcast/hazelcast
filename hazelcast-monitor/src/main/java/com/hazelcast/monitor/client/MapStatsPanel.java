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
import com.google.gwt.user.client.ui.AbsolutePanel;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.hazelcast.monitor.client.event.ChangeEvent;
import com.hazelcast.monitor.client.event.MapStatistics;

import java.util.Collection;

import static com.hazelcast.monitor.client.PanelUtils.createFormattedFlexTable;
import static com.hazelcast.monitor.client.PanelUtils.formatEvenRows;
import static com.hazelcast.monitor.client.PanelUtils.removeUnusedRows;

public abstract class MapStatsPanel extends MapPanel implements MonitoringPanel {

    public MapStatsPanel(String name, AsyncCallback<ChangeEvent> callBack, String panelLabel, HazelcastServiceAsync hazelcastServiceAsync) {
        super(name, callBack, panelLabel, hazelcastServiceAsync);
    }

    public void handle(ChangeEvent e) {

        MapStatistics event = (MapStatistics) e;
        if(super.mapName==null || !super.mapName.equals(event.getMapName())){
            return;
        }
        VerticalPanel vPanel = (VerticalPanel) disclosurePanel.getContent();
        AbsolutePanel absTablePanel = (AbsolutePanel) vPanel.getWidget(1);
        FlexTable table = (FlexTable) absTablePanel.getWidget(0);
        int row = 1;
        Collection<MapStatistics.LocalMapStatistics> collection = event.getListOfLocalStats();
        for (MapStatistics.LocalMapStatistics localMapStatistics : collection) {
            handleRow(table, row, localMapStatistics);
            formatEvenRows(row, table);
            row++;
        }
        removeUnusedRows(row, table);
    }

    protected abstract void handleRow(FlexTable table, int row, MapStatistics.LocalMapStatistics localMapStatistics);

    @Override
    protected FlexTable createTable() {
        FlexTable table = createFormattedFlexTable();
        createColumns(table);
        return table;
    }

    protected abstract void createColumns(FlexTable table);

    static String formatMemorySize(long size) {
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

    static String toPrecision(double dbl) {
        int ix = (int) (dbl * 100.0); // scale it
        double dbl2 = ((double) ix) / 100.0;
        return String.valueOf(dbl2);
    }
}
