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

import com.google.gwt.core.client.GWT;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.*;
import com.hazelcast.monitor.client.event.ChangeEvent;
import com.hazelcast.monitor.client.event.ChangeEventType;
import com.hazelcast.monitor.client.event.MapStatistics;

import java.util.ArrayList;
import java.util.List;

public class MapStatisticsPanel implements MonitoringPanel {
    DisclosurePanel dsp;

    private String mapName;
    private AsyncCallback<ChangeEvent> callBack;

    private final HazelcastServiceAsync hazelcastService = GWT
            .create(HazelcastService.class);

    public MapStatisticsPanel(String name, AsyncCallback<ChangeEvent> callBack) {
        this.mapName = name;
        this.callBack = callBack;
        dsp = initPanel();

    }

    public Widget getDsp() {
        return dsp;
    }


    public boolean register(ClusterWidgets clusterWidgets) {
        List<MonitoringPanel> list = clusterWidgets.getPanels().get(ChangeEventType.MAP_STATISTICS);
        if (list == null) {
            list = new ArrayList<MonitoringPanel>();
            clusterWidgets.getPanels().put(ChangeEventType.MAP_STATISTICS, list);
        }
        if (!list.contains(this)) {
            list.add(this);
        }
        hazelcastService.registerEvent(ChangeEventType.MAP_STATISTICS, clusterWidgets.clusterId, mapName, callBack);
        return true;
    }

    public boolean deRegister(ClusterWidgets clusterWidgets) {
        List<MonitoringPanel> list = clusterWidgets.getPanels().get(ChangeEventType.MAP_STATISTICS);
        if (list == null) {
            return false;
        }
        hazelcastService.deRegisterEvent(ChangeEventType.MAP_STATISTICS, clusterWidgets.clusterId, mapName, new AsyncCallback<Void>() {

            public void onFailure(Throwable caught) {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            public void onSuccess(Void result) {
                //To change body of implemented methods use File | Settings | File Templates.
            }
        });
        return list.remove(this);
    }

    public void handle(ChangeEvent e) {
        MapStatistics event = (MapStatistics) e;
        int size = event.getSize();
        VerticalPanel vPanel = (VerticalPanel) dsp.getContent();
        Label label = (Label) vPanel.getWidget(0);
        label.setText("Size is: " + size);

        Image image = (Image) vPanel.getWidget(1);
        image.setUrl("/ChartGenerator?id = " + Math.random() * 10);
    }

    private DisclosurePanel initPanel() {
        final DisclosurePanel disclosurePanel = new DisclosurePanel("Map Statistics");
        VerticalPanel vPanel = new VerticalPanel();
        vPanel.add(new Label());
        vPanel.add(new Image());
        disclosurePanel.add(vPanel);
        disclosurePanel.setOpen(true);
        return disclosurePanel;
    }
}
