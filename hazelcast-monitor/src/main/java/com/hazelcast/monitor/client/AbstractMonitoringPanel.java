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
import com.google.gwt.user.client.ui.Label;
import com.hazelcast.monitor.client.event.ChangeEvent;
import com.hazelcast.monitor.client.event.ChangeEventType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractMonitoringPanel implements MonitoringPanel {

    private final HazelcastServiceAsync hazelcastService;

    protected AbstractMonitoringPanel(HazelcastServiceAsync hazelcastService) {
        this.hazelcastService = hazelcastService;
    }

    /**
     * Registers the panel in ClusterWidgets;
     * If it existed before, returns false. Otherwise returns true.
     *
     * @param clusterWidgets
     * @param changeEventType
     * @return
     */
    protected boolean register(ClusterWidgets clusterWidgets, ChangeEventType changeEventType) {
        List<MonitoringPanel> list = clusterWidgets.getPanels().get(changeEventType);
        boolean result;
        if (list == null) {
            list = new ArrayList<MonitoringPanel>();
            clusterWidgets.getPanels().put(changeEventType, list);
        }
        result = list.isEmpty();
        list.add(this);
        return result;

    }

    public boolean register(ClusterWidgets clusterWidgets, ChangeEventType eventType, String name, AsyncCallback<ChangeEvent> callBack) {
        boolean newEvent = register(clusterWidgets, eventType);
        if (newEvent) {
            hazelcastService.registerEvent(eventType, clusterWidgets.clusterId, name, callBack);
        }
        return true;
    }

    /**
     * De registers the panel from clusterWidgets. Returns true of there is no registered panel
     * with same ChangeEventType.
     *
     * @param clusterWidgets
     * @param changeEventType
     * @return
     */
    protected boolean deRegister(ClusterWidgets clusterWidgets, ChangeEventType changeEventType) {
        List<MonitoringPanel> list = clusterWidgets.getPanels().get(changeEventType);
        if (list != null) {
            list.remove(this);
        }
        return list.isEmpty();
    }

    public boolean deRegister(ClusterWidgets clusterWidgets, ChangeEventType changeEventType, String name) {
        boolean isEmpty = deRegister(clusterWidgets, changeEventType);
        if (isEmpty) {
            hazelcastService.deRegisterEvent(changeEventType, clusterWidgets.clusterId, name, new AsyncCallback<Void>() {

                public void onFailure(Throwable throwable) {
                    //To change body of implemented methods use File | Settings | File Templates.
                }

                public void onSuccess(Void aVoid) {
                    //To change body of implemented methods use File | Settings | File Templates.
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
