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

import com.google.gwt.core.client.GWT;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.*;
import com.hazelcast.monitor.client.event.ChangeEvent;
import com.hazelcast.monitor.client.event.ChangeEventType;
import com.hazelcast.monitor.client.event.Partitions;

import static com.hazelcast.monitor.client.PanelUtils.*;

public class PartitionsPanel extends AbstractMonitoringPanel implements MonitoringPanel {

    final private AsyncCallback<ChangeEvent> callBack;
    final AbsolutePanel absTablePanel;
    private ClusterWidgets clusterWidgets;
    protected final HazelcastServiceAsync hazelcastService = GWT
            .create(HazelcastService.class);

    public PartitionsPanel(AsyncCallback<ChangeEvent> callBack) {
        this.callBack = callBack;
        absTablePanel = new AbsolutePanel();
        absTablePanel.addStyleName("img-shadow");
        FlexTable table = createFormattedFlexTable();
        table.setText(0, 0, "Member");
        table.setText(0, 1, "Count");
        table.setText(0, 2, "Partitions");
        absTablePanel.add(table);
    }

    public void handle(ChangeEvent event) {
        Partitions partitionsChangeEvent = (Partitions) event;
        int row = 1;
        FlexTable table = (FlexTable) absTablePanel.getWidget(0);
        TreeItem memberTreeItem = clusterWidgets.getMemberTreeItem();
        for (int i = 0; i < memberTreeItem.getChildCount(); i++) {
            TreeItem treeItem = memberTreeItem.getChild(i);
            Anchor link = (Anchor) treeItem.getWidget();
            String member = link.getText();
            table.setWidget(row, 0, clusterWidgets.getInstanceLink(null, member));
            Integer size = partitionsChangeEvent.getCount().get(member);
            size = (size == null) ? 0 : size;
            table.setText(row, 1, String.valueOf(size));
            table.setText(row, 2, partitionsChangeEvent.getPartitions().get(member));
            formatEvenRows(row, table);
            row++;
        }
        removeUnusedRows(row, table);
    }

    public Widget getPanelWidget() {
        return absTablePanel;
    }

    public boolean register(ClusterWidgets clusterWidgets) {
        this.clusterWidgets = clusterWidgets;
        return super.register(clusterWidgets, ChangeEventType.PARTITIONS, null, callBack);
    }

    public boolean deRegister(ClusterWidgets clusterWidgets) {
        return super.deRegister(clusterWidgets, ChangeEventType.PARTITIONS, null);
    }
}
