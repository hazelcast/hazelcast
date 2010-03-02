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

public class MapEntryOwnerShipPanel extends MapStatsPanel implements MonitoringPanel {

    public MapEntryOwnerShipPanel(String name, AsyncCallback<ChangeEvent> callBack, ServicesFactory servicesFactory) {
        super(name, callBack, "Size & Memory Details", servicesFactory.getHazelcastService());
    }

    @Override
    protected void handleRow(FlexTable table, int row, MapStatistics.LocalMapStatistics localMapStatistics) {
        table.setWidget(row, 0, clusterWidgets.getInstanceLink(null, localMapStatistics.memberName));
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
    }

    @Override
    protected void createColumns(FlexTable table) {
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
    }
}
