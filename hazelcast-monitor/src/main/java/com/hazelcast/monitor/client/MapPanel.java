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

import com.google.gwt.event.dom.client.MouseOutEvent;
import com.google.gwt.event.dom.client.MouseOutHandler;
import com.google.gwt.event.dom.client.MouseOverEvent;
import com.google.gwt.event.dom.client.MouseOverHandler;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.*;
import com.hazelcast.monitor.client.event.ChangeEvent;
import com.hazelcast.monitor.client.event.ChangeEventType;

import java.util.HashMap;
import java.util.Map;

public abstract class MapPanel extends AbstractMonitoringPanel implements MonitoringPanel {
    final protected String mapName;
    final protected AsyncCallback<ChangeEvent> callBack;
    DisclosurePanel disclosurePanel;
    protected ClusterWidgets clusterWidgets;
    final private String panelHeader;
    final HazelcastServiceAsync hazelcastService;

    public MapPanel(String name, AsyncCallback<ChangeEvent> callBack, String panelLabel, HazelcastServiceAsync hazelcastService) {
        super(hazelcastService);
        this.mapName = name;
        this.callBack = callBack;
        panelHeader = panelLabel;
        this.hazelcastService = hazelcastService;
    }

    protected DisclosurePanel initPanel(String panelHeader) {
        final DisclosurePanel disclosurePanel = new DisclosurePanel(panelHeader);
        VerticalPanel vPanel = new VerticalPanel();
        vPanel.add(new Label());
        Widget widget = createTable();
        if (widget != null) {
            AbsolutePanel absTablePanel = new AbsolutePanel();
            absTablePanel.addStyleName("img-shadow");
            absTablePanel.add(widget);
            vPanel.add(absTablePanel);
        }
        disclosurePanel.add(vPanel);
        disclosurePanel.setOpen(false);
        return disclosurePanel;
    }

    protected abstract FlexTable createTable();

    public Widget getPanelWidget() {
        if (disclosurePanel == null) {
            synchronized (mapName) {
                if (disclosurePanel == null) {
                    disclosurePanel = initPanel(panelHeader);
                }
            }
        }
        return disclosurePanel;
    }

    public boolean register(ClusterWidgets clusterWidgets) {
        this.clusterWidgets = clusterWidgets;
        Boolean result = super.register(clusterWidgets, ChangeEventType.MAP_STATISTICS, mapName, callBack);
        return result;
    }

    public boolean deRegister(final ClusterWidgets clusterWidgets) {
        return super.deRegister(clusterWidgets, ChangeEventType.MAP_STATISTICS, mapName);
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
