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
import com.google.gwt.user.client.ui.HasHorizontalAlignment;
import com.google.gwt.user.client.ui.Widget;
import com.hazelcast.monitor.client.event.ChangeEvent;
import com.hazelcast.monitor.client.event.ChangeEventType;
import com.hazelcast.monitor.client.event.MemberInfo;

import java.util.HashMap;
import java.util.Set;

import static com.hazelcast.monitor.client.MapStatsPanel.formatMemorySize;
import static com.hazelcast.monitor.client.PanelUtils.createFormattedFlexTable;

public class MemberPanel extends AbstractMonitoringPanel implements MonitoringPanel {
    final FlexTable table;
    final AbsolutePanel absTablePanel;
    final private AsyncCallback<ChangeEvent> callBack;
    final private String memberName;
    volatile boolean set = false;

    public MemberPanel(AsyncCallback<ChangeEvent> callBack, String name, ServicesFactory servicesFactory) {
        super(servicesFactory.getHazelcastService());
        this.callBack = callBack;
        absTablePanel = new AbsolutePanel();
        absTablePanel.addStyleName("img-shadow");
        table = createFormattedFlexTable();
        this.memberName = name;
        absTablePanel.add(table);
        table.setText(0, 0, "System & Runtime Properties");
        table.setText(1, 0, "Partition Count");
        table.setText(2, 0, "Partitions");
        table.setText(3, 0, "Maximum Memory");
        table.setText(4, 0, "Total Memory");
        table.setText(5, 0, "Used Memory");
        table.setText(6, 0, "Free Memory");
        table.setText(7, 0, "Available Processors");
        table.getFlexCellFormatter().setColSpan(0,0,2);
        table.setWidth("1000px");
    }

    public void handle(ChangeEvent event) {
        MemberInfo memberInfo = (MemberInfo) event;
        Set<Integer> partitions = memberInfo.getPartitions();
        StringBuilder blocks = new StringBuilder();
        int size = partitions.size();
        int counter = 0;
        for (int partition : partitions) {
            blocks.append(partition);
            if (++counter != size) {
                blocks.append(", ");
            }
        }
        table.setText(1, 1, String.valueOf(size));
        table.setText(2, 1, blocks.toString());
        table.setText(3, 1, formatMemorySize(memberInfo.getMaxMemory()));
        table.setText(4, 1, formatMemorySize(memberInfo.getTotalMemory()));
        table.setText(5, 1, formatMemorySize(memberInfo.getTotalMemory() - memberInfo.getFreeMemory()));
        table.setText(6, 1, formatMemorySize(memberInfo.getFreeMemory()));
        table.setText(7, 1, String.valueOf(memberInfo.getAvailableProcessors()));
        HashMap props = memberInfo.getSystemProps();
        int i = 7;
        if (!set) {
            for (Object key : props.keySet()) {
                i++;
                table.setText(i, 0, String.valueOf(key));
                String value = String.valueOf(props.get(key));
                if(value.length() > 100){
                    String newValue = value.replaceAll(":",": ");
                    value = newValue;
                }
                table.setText(i, 1, value);
            }
            set = true;
        }
    }

    public Widget getPanelWidget() {
        return absTablePanel;
    }

    public boolean register(ClusterWidgets clusterWidgets) {
        return super.register(clusterWidgets, ChangeEventType.MEMBER_INFO, memberName, callBack);
    }

    public boolean deRegister(ClusterWidgets clusterWidgets) {
        return super.deRegister(clusterWidgets, ChangeEventType.MEMBER_INFO, memberName);
    }
}
