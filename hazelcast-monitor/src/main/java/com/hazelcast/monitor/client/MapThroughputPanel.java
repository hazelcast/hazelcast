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
        table.setWidget(row, 0, clusterWidgets.getInstanceLink(null, localMapStatistics.memberName));
        table.setText(row, 1, "" + localMapStatistics.totalOperationsInSec());
        table.setText(row, 2, "" + localMapStatistics.numberOfGetsInSec);
        table.setText(row, 3, "" + localMapStatistics.numberOfPutsInSec);
        table.setText(row, 4, "" + localMapStatistics.numberOfRemovesInSec);
        table.setText(row, 5, "" + localMapStatistics.numberOfOthersInSec);
        table.setText(row, 6, "" + localMapStatistics.hits);
        table.getColumnFormatter().addStyleName(0, "mapstatsStringColumn");
        table.getCellFormatter().addStyleName(row, 1, "mapstatsNumericColumn");
        table.getCellFormatter().addStyleName(row, 2, "mapstatsNumericColumn");
        table.getCellFormatter().addStyleName(row, 3, "mapstatsNumericColumn");
        table.getCellFormatter().addStyleName(row, 4, "mapstatsNumericColumn");
        table.getCellFormatter().addStyleName(row, 5, "mapstatsNumericColumn");
        table.getCellFormatter().addStyleName(row, 6, "mapstatsNumericColumn");
    }

    @Override
    protected void createColumns(FlexTable table) {
        table.setWidget(0, 0, new LabelWithToolTip("Members", "Members of the Cluster"));
        table.setWidget(0, 1, new LabelWithToolTip("Total Throughput", "Number of total operations PS"));
        table.setWidget(0, 2, new LabelWithToolTip("Gets", "Number of Get operations PS"));
        table.setWidget(0, 3, new LabelWithToolTip("Puts", "Number of Put operations PS"));
        table.setWidget(0, 4, new LabelWithToolTip("Removes", "Number of Remove operations PS"));
        table.setWidget(0, 5, new LabelWithToolTip("Other Operations", "All other operations PS"));
        table.setWidget(0, 6, new LabelWithToolTip("Total Hits", "Number of Hits"));
    }
}
