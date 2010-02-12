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
import com.google.gwt.event.dom.client.MouseOutEvent;
import com.google.gwt.event.dom.client.MouseOutHandler;
import com.google.gwt.event.dom.client.MouseOverEvent;
import com.google.gwt.event.dom.client.MouseOverHandler;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.*;
import com.hazelcast.monitor.client.event.ChangeEvent;
import com.hazelcast.monitor.client.event.ChangeEventType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class MapPanel extends AbstractMonitoringPanel implements MonitoringPanel {
    final protected String mapName;
    final protected AsyncCallback<ChangeEvent> callBack;
    DisclosurePanel disclosurePanel;
    protected final HazelcastServiceAsync hazelcastService = GWT
            .create(HazelcastService.class);
    protected ClusterWidgets clusterWidgets;

    public MapPanel(String name, AsyncCallback<ChangeEvent> callBack, String panelLabel) {
        this.mapName = name;
        this.callBack = callBack;
        disclosurePanel = initPanel(panelLabel);
        disclosurePanel.setOpen(false);        
    }

    protected DisclosurePanel initPanel(String panelHeader) {
        final DisclosurePanel disclosurePanel = new DisclosurePanel(panelHeader);
        VerticalPanel vPanel = new VerticalPanel();
        vPanel.add(new Label());
        FlexTable table = createTable();
        if (table != null) {
            AbsolutePanel absTablePanel = new AbsolutePanel();
            absTablePanel.addStyleName("img-shadow");
            absTablePanel.add(table);
            vPanel.add(absTablePanel);
        }
        disclosurePanel.add(vPanel);
        disclosurePanel.setOpen(true);
        return disclosurePanel;
    }

    protected abstract FlexTable createTable();

    public Widget getPanelWidget() {
        return disclosurePanel;
    }

    public boolean register(ClusterWidgets clusterWidgets) {
        this.clusterWidgets = clusterWidgets;
        super.register(clusterWidgets, ChangeEventType.MAP_STATISTICS);
        List<ChangeEventType> registeredChangeEvents = clusterWidgets.getRegisteredChangeEvents(mapName);
        if (!registeredChangeEvents.contains(ChangeEventType.MAP_STATISTICS)) {
            hazelcastService.registerEvent(ChangeEventType.MAP_STATISTICS, clusterWidgets.clusterId, mapName, callBack);
            registeredChangeEvents.add(ChangeEventType.MAP_STATISTICS);
        }
        return true;
    }

    public boolean deRegister(final ClusterWidgets clusterWidgets) {
        boolean isEmpty = super.deRegister(clusterWidgets, ChangeEventType.MAP_STATISTICS);
        if (isEmpty) {
            hazelcastService.deRegisterEvent(ChangeEventType.MAP_STATISTICS, clusterWidgets.clusterId, mapName, new AsyncCallback<Void>() {

                public void onFailure(Throwable caught) {
                }

                public void onSuccess(Void result) {
                    clusterWidgets.getRegisteredChangeEvents(mapName).remove(ChangeEventType.MAP_STATISTICS);
                }
            });
        }
        return true;
    }

    public static class LabelWithToolTip extends Label {
        public LabelWithToolTip(final String label, final String toolTip) {
            super(label);
            final Map<Integer, ToolTip> map = new HashMap();
            this.addMouseOverHandler(new MouseOverHandler() {
                public void onMouseOver(MouseOverEvent event) {
                    String tip = toolTip;
                    if (tip == null || tip.equals("")) {
                        tip = label;
                    }
                    ToolTip ttip = new ToolTip(tip, event.getClientX(), event.getClientY());
                    map.put(1, ttip);
                }
            });
            this.addMouseOutHandler(new MouseOutHandler() {
                public void onMouseOut(MouseOutEvent event) {
                    ToolTip tip = map.remove(1);
                    if (tip != null) {
                        tip.hide();
                    }
                }
            });
        }
    }
}
