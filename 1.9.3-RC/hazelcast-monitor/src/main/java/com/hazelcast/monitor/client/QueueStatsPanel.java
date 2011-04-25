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
import com.hazelcast.monitor.client.event.QueueStatistics;

public class QueueStatsPanel extends InstanceStatsPanel {
    public QueueStatsPanel(String name, AsyncCallback<? extends ChangeEvent> callBack, HazelcastServiceAsync hazelcastService) {
        super(name, callBack, "Queue Statistics", hazelcastService, ChangeEventType.QUEUE_STATISTICS);
    }

    @Override
    protected void handleRow(FlexTable table, int row, LocalInstanceStatistics localInstanceStatistics) {
        handleRow(table, row, (QueueStatistics.LocalQueueStatistics) localInstanceStatistics);
    }

    protected void handleRow(FlexTable table, int row, QueueStatistics.LocalQueueStatistics localQueueStatistics) {
        table.setText(row, 0, "" + row);
        table.setWidget(row, 1, clusterWidgets.getInstanceLink(null, localQueueStatistics.memberName));
        table.setText(row, 2, "" + localQueueStatistics.ownedItemCount);
        table.setText(row, 3, "" + localQueueStatistics.backupItemCount);
        table.setText(row, 4, ((localQueueStatistics.minAge == Long.MAX_VALUE) ? "-" : "" + localQueueStatistics.minAge));
        table.setText(row, 5, ((localQueueStatistics.maxAge == Long.MIN_VALUE) ? "-" : "" + localQueueStatistics.maxAge));
        table.setText(row, 6, ((localQueueStatistics.aveAge == 0) ? "-" : "" + localQueueStatistics.aveAge));
        table.setText(row, 7, "" + localQueueStatistics.numberOfOffersInSec);
        table.setText(row, 8, "" + localQueueStatistics.numberOfRejectedOffersInSec);
        table.setText(row, 9, "" + localQueueStatistics.numberOfPollsInSec);
        table.setText(row, 10, "" + localQueueStatistics.numberOfEmptyPollsInSec);
        table.getColumnFormatter().addStyleName(0, "mapstatsNumericColumn");
        table.getColumnFormatter().addStyleName(1, "mapstatsStringColumn");
        for (int i = 2; i < 11; i++) {
            table.getCellFormatter().addStyleName(row, i, "mapstatsNumericColumn");
        }
    }

    @Override
    protected void createColumns(FlexTable table) {
        table.setWidget(0, 0, new LabelWithToolTip("#", ""));
        table.setWidget(0, 1, new LabelWithToolTip("Members", "Members of the Cluster"));
        table.setWidget(0, 2, new LabelWithToolTip("Items", "Owned Item Count"));
        table.setWidget(0, 3, new LabelWithToolTip("Backups", "Backup Item Count"));
        table.setWidget(0, 4, new LabelWithToolTip("Min Age", "Min Age of the Items in This Member in Milliseconds"));
        table.setWidget(0, 5, new LabelWithToolTip("Max Age", "Max Age of the Items in This Member in Milliseconds"));
        table.setWidget(0, 6, new LabelWithToolTip("Average Age", "Average Age of the Items in This Member in Milliseconds"));
        table.setWidget(0, 7, new LabelWithToolTip("Offers", "Total Number of Offers in Sec"));
        table.setWidget(0, 8, new LabelWithToolTip("Rejected Offers", "Number of Rejected Offers in Sec"));
        table.setWidget(0, 9, new LabelWithToolTip("Polls", "Total Number of Polls in Sec"));
        table.setWidget(0, 10, new LabelWithToolTip("Empty Polls", "Number of Empty Polls in Sec"));
    }
}
