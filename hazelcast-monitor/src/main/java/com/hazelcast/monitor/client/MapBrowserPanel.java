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
import com.google.gwt.i18n.client.DateTimeFormat;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.*;
import com.hazelcast.monitor.client.event.ChangeEvent;
import com.hazelcast.monitor.client.event.ChangeEventType;
import com.hazelcast.monitor.client.exception.ClientDisconnectedException;

import java.util.Date;

import static com.hazelcast.monitor.client.MapEntryOwnerShipPanel.formatMemorySize;

public class MapBrowserPanel extends MapPanel implements MonitoringPanel {
    private String name;

    protected final MapServiceAsync mapService;

    public MapBrowserPanel(final String name, AsyncCallback<ChangeEvent> callBack, ServicesFactory servicesFactory) {
        super(name, callBack, "Map Browser", servicesFactory.getHazelcastService(), ChangeEventType.MAP_STATISTICS);
        mapService = servicesFactory.getMapServiceAsync();
        this.name = name;
    }

    private String format(DateTimeFormat dateFormatter, Date date) {
        if (date.getTime() == 0) {
            return "";
        } else if (date.getTime() == Long.MAX_VALUE) {
            return "";
        }
        return dateFormatter.format(date);
    }

    public void handle(ChangeEvent event) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    protected FlexTable createTable() {
        FlexTable table = new FlexTable();
        table.addStyleName("table");
        table.addStyleName("mapstats");
        FlexTable getTable = new FlexTable();
        table.setWidget(0, 0, new Label(""));
        table.getFlexCellFormatter().setColSpan(0, 0, 2);
        table.setWidget(1, 1, getTable);
        final TextBox key = new TextBox();
        getTable.setWidget(0, 0, new Label("Key: "));
        getTable.setWidget(0, 1, key);
        Button button = new Button("Get");
        button.addStyleName("map_get_button");
        getTable.setWidget(0, 2, button);
        FlexTable resultTable = new FlexTable();
        resultTable.setWidth("500px");
        table.setWidget(1, 2, resultTable);
        resultTable.setWidget(0, 0, new Label("Value: "));
        resultTable.setWidget(1, 0, new Label("Hits: "));
        resultTable.setWidget(2, 0, new Label("Cost: "));
        resultTable.setWidget(3, 0, new Label("Valid: "));
        resultTable.setWidget(4, 0, new Label("Expiration Time: "));
        resultTable.setWidget(5, 0, new Label("Last Acess Time: "));
        resultTable.setWidget(6, 0, new Label("Last Update Time: "));
        resultTable.setWidget(7, 0, new Label("Creation Time: "));
        for (int i = 0; i < resultTable.getRowCount(); i++) {
            Label label = (Label) resultTable.getWidget(i, 0);
            label.addStyleName("bold");
            if (i % 2 == 1) {
                resultTable.getRowFormatter().addStyleName(i, "mapstatsEvenRow");
            }
        }
        final TextArea value = new TextArea();
        final Label hits = new Label();
        final Label cost = new Label();
        final Label expirationTime = new Label();
        final Label lastAcessTime = new Label();
        final Label lastUpdateTime = new Label();
        final Label creationTime = new Label();
        final Label valid = new Label();
        resultTable.setWidget(0, 1, value);
        resultTable.setWidget(1, 1, hits);
        resultTable.setWidget(2, 1, cost);
        resultTable.setWidget(3, 1, valid);
        resultTable.setWidget(4, 1, expirationTime);
        resultTable.setWidget(5, 1, lastAcessTime);
        resultTable.setWidget(6, 1, lastUpdateTime);
        resultTable.setWidget(7, 1, creationTime);
        final DateTimeFormat dateFormatter = DateTimeFormat.getFormat("yyyy.MM.dd HH:mm:ss");
        button.addClickHandler(new ClickHandler() {

            public void onClick(ClickEvent clickEvent) {
                mapService.get(clusterWidgets.clusterId, name, key.getText(), new AsyncCallback<MapEntry>() {
                    public void onFailure(Throwable throwable) {
                        if (throwable instanceof ClientDisconnectedException) {
                            clusterWidgets.disconnected();
                        }
                        value.setText(throwable.toString());
                    }

                    public void onSuccess(MapEntry mapEntry) {
                        value.setText((mapEntry == null) ? "null" : mapEntry.getValue());
                        hits.setText((mapEntry == null) ? "" : String.valueOf(mapEntry.getHits()));
                        cost.setText((mapEntry == null) ? "" : formatMemorySize(mapEntry.getCost()));
                        valid.setText((mapEntry == null) ? "" : String.valueOf(mapEntry.isValid()));
                        expirationTime.setText((mapEntry == null) ? "" : format(dateFormatter, mapEntry.getExpirationTime()));
                        lastAcessTime.setText((mapEntry == null) ? "" : format(dateFormatter, mapEntry.getLastAccessTime()));
                        lastUpdateTime.setText((mapEntry == null) ? "" : format(dateFormatter, mapEntry.getLastUpdateTime()));
                        creationTime.setText((mapEntry == null) ? "" : format(dateFormatter, mapEntry.getCreationTime()));
                    }
                });
            }
        });
        return table;
    }
}
