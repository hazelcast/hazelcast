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
import com.hazelcast.monitor.client.event.ChangeEventType;
import com.hazelcast.monitor.client.event.InstanceStatistics;
import com.hazelcast.monitor.client.event.LocalInstanceStatistics;

import java.util.Collection;

import static com.hazelcast.monitor.client.PanelUtils.*;

public abstract class InstanceStatsPanel extends InstancePanel {
    public InstanceStatsPanel(String name, AsyncCallback<? extends ChangeEvent> callBack, String panelLabel, HazelcastServiceAsync hazelcastService, ChangeEventType changeEventType) {
        super(name, callBack, panelLabel, hazelcastService, changeEventType);
    }

    @Override
    protected FlexTable createTable() {
        FlexTable table = createFormattedFlexTable();
        createColumns(table);
        return table;
    }

    protected abstract void createColumns(FlexTable table);

    public void handle(ChangeEvent e) {
        if (!disclosurePanel.isOpen()) {
            return;
        }
        InstanceStatistics event = (InstanceStatistics) e;
        if (super.name == null || !super.name.equals(event.getName())) {
            return;
        }
        VerticalPanel vPanel = (VerticalPanel) disclosurePanel.getContent();
        AbsolutePanel absTablePanel = (AbsolutePanel) vPanel.getWidget(1);
        FlexTable table = (FlexTable) absTablePanel.getWidget(0);
        int row = 1;
        Collection<? extends LocalInstanceStatistics> collection = event.getListOfLocalStats();
        for (LocalInstanceStatistics localInstanceStatistics : collection) {
            handleRow(table, row, localInstanceStatistics);
            formatEvenRows(row, table);
            row++;
        }
        removeUnusedRows(row, table);
    }

    protected abstract void handleRow(FlexTable table, int row, LocalInstanceStatistics localInstanceStatistics);
}
