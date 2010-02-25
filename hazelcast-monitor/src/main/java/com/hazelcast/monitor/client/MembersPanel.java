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

import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.AbsolutePanel;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.Widget;
import com.hazelcast.monitor.client.event.ChangeEvent;
import com.hazelcast.monitor.client.event.ChangeEventType;
import com.hazelcast.monitor.client.event.Partitions;

import static com.hazelcast.monitor.client.PanelUtils.createFormattedFlexTable;

public class MembersPanel extends AbstractMonitoringPanel implements MonitoringPanel {
    final FlexTable table;
    final AbsolutePanel absTablePanel;
    final private AsyncCallback<ChangeEvent> callBack;
    final private String memberName;

    public MembersPanel(AsyncCallback<ChangeEvent> callBack, String name, ServicesFactory servicesFactory) {
        super(servicesFactory.getHazelcastService());
        this.callBack = callBack;
        absTablePanel = new AbsolutePanel();
        absTablePanel.addStyleName("img-shadow");
        table = createFormattedFlexTable();
        this.memberName = name;
        absTablePanel.add(table);
        table.setText(0, 0, "Count");
        table.setText(0, 1, "Partitions");
    }

    public void handle(ChangeEvent event) {
        Partitions partitionsChangeEvent = (Partitions) event;
        String blocks = partitionsChangeEvent.getPartitions().get(memberName);
        table.setText(1, 0, String.valueOf(partitionsChangeEvent.getCount().get(memberName)));
        table.setText(1, 1, blocks);
    }

    public Widget getPanelWidget() {
        return absTablePanel;
    }

    public boolean register(ClusterWidgets clusterWidgets) {
        return super.register(clusterWidgets, ChangeEventType.PARTITIONS, null, callBack);
    }

    public boolean deRegister(ClusterWidgets clusterWidgets) {
        return super.deRegister(clusterWidgets, ChangeEventType.PARTITIONS, null);
    }
}
