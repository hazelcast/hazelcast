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
import com.hazelcast.monitor.client.event.MapStatistics;

public class MapThroughputPanel extends MapStatsPanel {
    public MapThroughputPanel(String name, AsyncCallback<ChangeEvent> callBack, ServicesFactory servicesFactory) {
        super(name, callBack, "Throughput Details", servicesFactory.getHazelcastService());
    }

    @Override
    protected void handleRow(FlexTable table, int row, MapStatistics.LocalMapStatistics localMapStatistics) {
        table.setText(row, 0, "" + row);
        table.setWidget(row, 1, clusterWidgets.getInstanceLink(null, localMapStatistics.memberName));
        table.setText(row, 2, "" + localMapStatistics.totalOperationsInSec());
        table.setText(row, 3, "" + localMapStatistics.numberOfGetsInSec);
        table.setText(row, 4, "" + localMapStatistics.numberOfPutsInSec);
        table.setText(row, 5, "" + localMapStatistics.numberOfRemovesInSec);
        table.setText(row, 6, "" + localMapStatistics.numberOfOthersInSec);
        table.setText(row, 7, "" + localMapStatistics.numberOfEventsInSec);
        table.setText(row, 8, "" + localMapStatistics.hits);
        for (int i = 2; i < 9; i++) {
            table.getCellFormatter().addStyleName(row, i, "mapstatsNumericColumn");
        }
    }

    @Override
    protected void createColumns(FlexTable table) {
        table.setWidget(0, 0, new LabelWithToolTip("#", ""));
        table.setWidget(0, 1, new LabelWithToolTip("Members", "Members of the Cluster"));
        table.setWidget(0, 2, new LabelWithToolTip("Total Throughput", "Number of total operations PS"));
        table.setWidget(0, 3, new LabelWithToolTip("Gets", "Number of Get operations PS"));
        table.setWidget(0, 4, new LabelWithToolTip("Puts", "Number of Put operations PS"));
        table.setWidget(0, 5, new LabelWithToolTip("Removes", "Number of Remove operations PS"));
        table.setWidget(0, 6, new LabelWithToolTip("Other Operations", "All other operations PS"));
        table.setWidget(0, 7, new LabelWithToolTip("Events Received", "Total Number of Entry Events Received"));
        table.setWidget(0, 8, new LabelWithToolTip("Total Hits", "Number of Hits"));
    }
}
