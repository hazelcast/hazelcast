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
import com.hazelcast.monitor.client.event.MapStatistics;

public class MapChartPanel extends MapPanel implements MonitoringPanel {
    public MapChartPanel(String name, AsyncCallback<ChangeEvent> callBack) {
        super(name, callBack, "Charts for Map: "+ name);
        disclosurePanel.setOpen(true);
    }

    public void handle(ChangeEvent e) {
        MapStatistics event = (MapStatistics) e;
        int size = event.getSize();
        VerticalPanel vPanel = (VerticalPanel) disclosurePanel.getContent();
        Label label = (Label) vPanel.getWidget(0);
        label.setText("Map Size: " + size);
        if (vPanel.getWidgetCount() < 2) {
            AbsolutePanel absImagePanel = new AbsolutePanel();
            absImagePanel.addStyleName("img-shadow");
            absImagePanel.add(new Image());
            vPanel.add(absImagePanel);
        }
        AbsolutePanel absolutePanel = (AbsolutePanel) vPanel.getWidget(1);
        Image image = (Image) absolutePanel.getWidget(0);
        image.setUrl("ChartGenerator?name=" + mapName + "&random=" + Math.random() * 10);
    }

    @Override
    protected FlexTable createTable() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
