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

import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.*;
import com.hazelcast.monitor.client.event.ChangeEvent;
import com.hazelcast.monitor.client.event.ChangeEventType;
import com.hazelcast.monitor.client.exception.ClientDisconnectedException;

public class MapEntrySetBrowserPanel extends InstancePanel implements MonitoringPanel {
    private String name;

    protected final MapServiceAsync mapService;

    public MapEntrySetBrowserPanel(final String name, AsyncCallback<ChangeEvent> callBack, ServicesFactory servicesFactory) {
        super(name, callBack, "Map Entry Set Browser", servicesFactory.getHazelcastService(), ChangeEventType.MAP_STATISTICS);
        mapService = servicesFactory.getMapServiceAsync();
        this.name = name;
    }

    public void handle(ChangeEvent event) {
    }

    @Override
    protected FlexTable createTable() {
        FlexTable table = new FlexTable();
        table.addStyleName("table");
        table.addStyleName("mapstats");
        table.setWidget(0, 0, new Label(""));
        table.getFlexCellFormatter().setColSpan(0, 0, 2);
        Button button = new Button("Refresh");
        table.setWidget(1, 0, button);
        final TextArea value = new TextArea();
        value.setVisible(false);
        table.setWidget(1, 1, value);
        button.addStyleName("map_refresh_button");
        final Grid resultTable = new Grid(2, 4);
        resultTable.setWidth("800px");
        table.setWidget(2, 0, resultTable);
        table.getFlexCellFormatter().setColSpan(2, 0, 2);
        resultTable.setText(0, 0, "Key:");
        resultTable.setText(0, 1, "Key class:");
        resultTable.setText(0, 2, "Value:");
        resultTable.setText(0, 3, "Value class:");
        resultTable.getRowFormatter().addStyleName(0, "mapstatsEvenRow");
        button.addClickHandler(new ClickHandler() {

            public void onClick(ClickEvent clickEvent) {
                mapService.getEntries(clusterWidgets.clusterId, name, new AsyncCallback<MapEntry[]>() {
                    public void onFailure(Throwable throwable) {
                        if (throwable instanceof ClientDisconnectedException) {
                            clusterWidgets.disconnected();
                        }
                        value.setVisible(true);
                        value.setText(throwable.toString());
                    }

                    public void onSuccess(MapEntry[] mapEntries) {
                        resultTable.resizeRows(mapEntries.length + 1);
                        value.setVisible(false);
                        int row = 1;
                        for (final MapEntry mapEntry : mapEntries) {
                            resultTable.setText(row, 0, mapEntry.getKey());
                            resultTable.setText(row, 1, mapEntry.getKeyClass());
                            resultTable.setText(row, 2, mapEntry.getValue());
                            resultTable.setText(row, 3, mapEntry.getValueClass());
                            if (row % 2 == 0) {
                                resultTable.getRowFormatter().addStyleName(row, "mapstatsEvenRow");
                            }
                            row++;
                        }
                    }
                });
            }
        });
        return table;
    }
}
