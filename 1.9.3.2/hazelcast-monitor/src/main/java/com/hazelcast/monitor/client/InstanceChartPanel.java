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

import com.google.gwt.http.client.URL;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.*;
import com.hazelcast.monitor.client.event.ChangeEvent;
import com.hazelcast.monitor.client.event.ChangeEventType;
import com.hazelcast.monitor.client.event.InstanceStatistics;

public abstract class InstanceChartPanel extends InstancePanel {
    public InstanceChartPanel(String name, AsyncCallback<ChangeEvent> callBack, ServicesFactory servicesFactory,
                              ChangeEventType changeEventType) {
        super(name, callBack, "Charts", servicesFactory.getHazelcastService(), changeEventType);
    }

    public void handle(ChangeEvent e) {
        InstanceStatistics event = (InstanceStatistics) e;
        if (super.name == null || !super.name.equals(event.getName())) {
            return;
        }
        VerticalPanel vPanel = (VerticalPanel) disclosurePanel.getContent();
        Label label = (Label) vPanel.getWidget(0);
        label.setText("Size: " + event.getSize() + " | Total Throughput: " + event.getTotalOPS());
        if (vPanel.getWidgetCount() < 2) {
            HorizontalPanel horizontalPanel = new HorizontalPanel();
            horizontalPanel.add(createAbsPanelWithImage());
            horizontalPanel.add(createAbsPanelWithImage());
            horizontalPanel.setBorderWidth(0);
            vPanel.add(horizontalPanel);
        }
        HorizontalPanel horizontalPanel = (HorizontalPanel) vPanel.getWidget(1);
        Image sizeChart = (Image) ((AbsolutePanel) horizontalPanel.getWidget(0)).getWidget(0);
        String encodeName = URL.encodeComponent(name);
        sizeChart.setUrl(getServletName() + "?name=" + encodeName + "&type=size&random=" + Math.random() * 10);
        Image opsChart = (Image) ((AbsolutePanel) horizontalPanel.getWidget(1)).getWidget(0);
        opsChart.setUrl(getServletName() + "?name=" + encodeName + "&type=ops&random=" + Math.random() * 10);
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

    public abstract String getServletName();
}
