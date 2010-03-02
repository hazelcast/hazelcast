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
    public MapChartPanel(String name, AsyncCallback<ChangeEvent> callBack, ServicesFactory servicesFactory) {
        super(name, callBack, "Charts", servicesFactory.getHazelcastService());
//        disclosurePanel.setOpen(true);
    }

    public void handle(ChangeEvent e) {
        MapStatistics event = (MapStatistics) e;
        int size = event.getSize();
        VerticalPanel vPanel = (VerticalPanel) disclosurePanel.getContent();
        Label label = (Label) vPanel.getWidget(0);
        label.setText("Map Size: " + size);
        if (vPanel.getWidgetCount() < 2) {
            HorizontalPanel horizontalPanel = new HorizontalPanel();
            horizontalPanel.add(createAbsPanelWithImage());
            horizontalPanel.add(createAbsPanelWithImage());
            horizontalPanel.setBorderWidth(0);
            vPanel.add(horizontalPanel);
        }
        HorizontalPanel horizontalPanel = (HorizontalPanel) vPanel.getWidget(1);
        Image sizeChart = (Image) ((AbsolutePanel) horizontalPanel.getWidget(0)).getWidget(0);
        sizeChart.setUrl("ChartGenerator?name=" + mapName + "&type=size&random=" + Math.random() * 10);
        Image opsChart = (Image) ((AbsolutePanel) horizontalPanel.getWidget(1)).getWidget(0);
        opsChart.setUrl("ChartGenerator?name=" + mapName + "&type=ops&random=" + Math.random() * 10);
    }

    private AbsolutePanel createAbsPanelWithImage() {
        AbsolutePanel absImagePanel = new AbsolutePanel();
        absImagePanel.addStyleName("img-shadow");
        absImagePanel.add(new Image());
        return absImagePanel;
    }

    @Override
    protected FlexTable createTable() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
