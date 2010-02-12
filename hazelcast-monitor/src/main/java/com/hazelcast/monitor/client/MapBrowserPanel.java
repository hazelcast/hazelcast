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
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.*;
import com.hazelcast.monitor.client.event.ChangeEvent;
import com.hazelcast.monitor.client.event.ChangeEventType;

public class MapBrowserPanel extends AbstractMonitoringPanel implements MonitoringPanel {
    final private String name;
    final private AsyncCallback<ChangeEvent> callBack;
    final private DisclosurePanel disclosurePanel;
    final private VerticalPanel verticalPanel;
    private ClusterWidgets clusterWidgets;

    protected final MapServiceAsync mapService = GWT
            .create(MapService.class);

    public MapBrowserPanel(final String name, AsyncCallback<ChangeEvent> callBack) {
        this.name = name;
        this.callBack = callBack;
        disclosurePanel = new DisclosurePanel("Map Browser");
        verticalPanel = new VerticalPanel();
        AbsolutePanel absolutePanel = new AbsolutePanel();
        absolutePanel.addStyleName("img-shadow");
        absolutePanel.addStyleName("table");
        FlexTable table = new FlexTable();
        table.addStyleName("table");
//        absolutePanel.add(verticalPanel);
        absolutePanel.add(table);
        disclosurePanel.add(absolutePanel);
//        HorizontalPanel horizontalPanel = new HorizontalPanel();
//        verticalPanel.add(horizontalPanel);
//        horizontalPanel.add(new Label("Key: "));
        table.setWidget(0,0,new Label("Key: "));
        final TextBox key = new TextBox();
//        horizontalPanel.add(key);
        table.setWidget(0,1,key);
        Button button = new Button("Get");
//        horizontalPanel.add(button);
        table.setWidget(0,2,button);
        table.getCellFormatter().setHorizontalAlignment(0,2, HasHorizontalAlignment.ALIGN_LEFT);
//        horizontalPanel.add(new Label("Value: "));
        table.setWidget(0,3, new Label("Value: "));
        final TextBox value = new TextBox();
        value.setText("");
        table.setWidget(0,4, value);
//        horizontalPanel.add(value);

        button.addClickHandler(new ClickHandler() {

            public void onClick(ClickEvent clickEvent) {
                mapService.get(clusterWidgets.clusterId, name, key.getText(), new AsyncCallback<String>() {
                    public void onFailure(Throwable throwable) {
                        value.setText(throwable.toString());
                    }

                    public void onSuccess(String s) {

                        value.setText((s==null)?"null":s);
                    }
                });
            }
        });
    }

    public void handle(ChangeEvent event) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public Widget getPanelWidget() {
        return disclosurePanel;
    }

    public boolean register(ClusterWidgets clusterWidgets) {
        this.clusterWidgets = clusterWidgets;
        super.register(clusterWidgets, ChangeEventType.MAP_STATISTICS);
        return true;
    }

    public boolean deRegister(ClusterWidgets clusterWidgets) {
        super.deRegister(clusterWidgets, ChangeEventType.MAP_STATISTICS);
        return true;
    }
}
