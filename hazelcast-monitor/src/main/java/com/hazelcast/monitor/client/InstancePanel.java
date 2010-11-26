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
import com.google.gwt.user.client.ui.*;
import com.hazelcast.monitor.client.event.ChangeEvent;
import com.hazelcast.monitor.client.event.ChangeEventType;

public abstract class InstancePanel extends AbstractMonitoringPanel implements MonitoringPanel {
    final protected String name;
    final protected AsyncCallback<? extends ChangeEvent> callBack;
    DisclosurePanel disclosurePanel;
    protected ClusterWidgets clusterWidgets;
    final private String panelHeader;
    final HazelcastServiceAsync hazelcastService;
    private ChangeEventType changeEventType;

    public InstancePanel(String name, AsyncCallback<? extends ChangeEvent> callBack, String panelLabel,
                         HazelcastServiceAsync hazelcastService, ChangeEventType changeEventType) {
        super(hazelcastService);
        this.name = name;
        this.callBack = callBack;
        panelHeader = panelLabel;
        this.hazelcastService = hazelcastService;
        this.changeEventType = changeEventType;
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

    protected abstract HTMLTable createTable();

    public Widget getPanelWidget() {
        if (disclosurePanel == null) {
            synchronized (name) {
                if (disclosurePanel == null) {
                    disclosurePanel = initPanel(panelHeader);
                }
            }
        }
        return disclosurePanel;
    }

    public boolean register(ClusterWidgets clusterWidgets) {
        this.clusterWidgets = clusterWidgets;
        return super.register(clusterWidgets, changeEventType, name, callBack);
    }

    public boolean deRegister(ClusterWidgets clusterWidgets) {
        return super.deRegister(clusterWidgets, changeEventType, name);
    }

    static String formatMemorySize(long size) {
        long tb = 1024l * 1024l * 1024l * 1024l;
        long gb = 1024 * 1024 * 1024;
        long mb = 1024 * 1024;
        long kb = 1024;
        double result;
        if ((result = (double) size / tb) >= 1) {
            return toPrecision(result) + " TB";
        } else if ((result = (double) size / gb) >= 1) {
            return toPrecision(result) + " GB";
        } else if ((result = (double) size / mb) >= 1) {
            return Math.round(result) + " MB";
        } else if ((result = (double) size / kb) >= 1) {
            return Math.round(result) + " KB";
        } else {
            return size + " Bytes";
        }
    }

    static String toPrecision(double dbl) {
        int ix = (int) (dbl * 100.0); // scale it
        double dbl2 = ((double) ix) / 100.0;
        return String.valueOf(dbl2);
    }
}
