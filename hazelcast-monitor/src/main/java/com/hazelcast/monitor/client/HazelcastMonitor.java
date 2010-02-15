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

import com.google.gwt.core.client.EntryPoint;
import com.google.gwt.core.client.GWT;
import com.google.gwt.event.logical.shared.ValueChangeEvent;
import com.google.gwt.event.logical.shared.ValueChangeHandler;
import com.google.gwt.user.client.History;
import com.google.gwt.user.client.Timer;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.*;
import com.hazelcast.monitor.client.event.ChangeEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
//import static com.hazelcast.monitor.client.AddClusterClickHandler.createClusterWidgets;

/**
 * Entry point classes define <code>onModuleLoad()</code>.
 */
public class HazelcastMonitor implements EntryPoint, ValueChangeHandler {
    private static final String LEFT_PANEL_SIZE = "230";
    private static final int REFRESH_INTERVAL = 1000;
    Map<Integer, ClusterWidgets> mapClusterWidgets = new HashMap<Integer, ClusterWidgets>();
    HorizontalSplitPanel mainPanel;
    DecoratedStackPanel dsPanel;
//    StackLayoutPanel dsPanel;
    private Timer refreshTimer;

    /**
     * Create a remote service proxy to talk to the server-side Hazelcast
     * service.
     */
    private final HazelcastServiceAsync hazelcastService = GWT
            .create(HazelcastService.class);

    /**
     * This is the entry point method.
     */
    public void onModuleLoad() {
        mainPanel = new HorizontalSplitPanel();
        mainPanel.setSplitPosition(LEFT_PANEL_SIZE);
        VerticalPanel leftPanel = new VerticalPanel();
        Image image = new Image("images/logo_3.png");
        leftPanel.add(image);
        DisclosurePanel clusterAddPanel = clusterAddPanel();
        leftPanel.add(clusterAddPanel);
        dsPanel = new DecoratedStackPanel();
//        dsPanel = new StackLayoutPanel(Style.Unit.EM);
        dsPanel.setWidth(LEFT_PANEL_SIZE);
        leftPanel.add(dsPanel);
        mainPanel.setLeftWidget(leftPanel);
        VerticalPanel rightPanel = new VerticalPanel();
        mainPanel.setRightWidget(rightPanel);
        RootPanel.get().add(mainPanel);
        History.addValueChangeHandler(this);
        hazelcastService.loadActiveClusterViews(new AsyncCallback<ArrayList<ClusterView>>() {

            public void onFailure(Throwable caught) {
            }

            public void onSuccess(ArrayList<ClusterView> result) {
                for (ClusterView cv : result) {
                    createAndAddClusterWidgets(cv);
                }
            }
        });
    }

    private DisclosurePanel clusterAddPanel() {
        final DisclosurePanel disclosurePanel = new DisclosurePanel(
                "Add Cluster to Monitor");
        final TextBox tbGroupName = new TextBox();
        tbGroupName.setText("dev");
        final TextBox tbGroupPass = new PasswordTextBox();
        tbGroupPass.setText("dev-pass");
        final TextBox tbAddresses = new TextBox();
//        tbAddresses.setText("192.168.1.3");
        final Label lbError = new Label("");
        lbError.setVisible(false);
        final Button btAddCluster = new Button("Add Cluster");
        btAddCluster.addClickHandler(new AddClusterClickHandler(this, tbGroupName, tbGroupPass, tbAddresses, lbError));
        VerticalPanel vPanel = new VerticalPanel();
        vPanel.add(tbGroupName);
        vPanel.add(tbGroupPass);
        vPanel.add(tbAddresses);
        vPanel.add(btAddCluster);
        vPanel.add(lbError);
        disclosurePanel.add(vPanel);
        disclosurePanel.setOpen(true);
        return disclosurePanel;
    }
    // Setup timer to refresh list automatically.

    public void onValueChange(final ValueChangeEvent event) {
        String token = event.getValue().toString();
        onValueChange(token);
    }

    public void onValueChange(final String token) {
        Map<String, String> map = parseParamString(token);
        String name = map.get("name");
        int clusterId = Integer.valueOf(map.get("clusterId"));
        String type = map.get("type");
        VerticalPanel panel = (VerticalPanel) mainPanel.getLeftWidget();
        ((DisclosurePanel) (panel.getWidget(1))).setOpen(false);
        AsyncCallback<ChangeEvent> callBack = new RegisterEventCallBack(this);
        ClusterWidgets clusterWidgets = mapClusterWidgets.get(clusterId);
        deRegisterAll();
        if ("MEMBER".equals(type)) {
            MembersPanel membersPanel = new MembersPanel(callBack, name);
            clusterWidgets.register(membersPanel);
        } else if ("PARTITIONS".equals(type)) {
            PartitionsPanel partitionsPanel = new PartitionsPanel(callBack);
            clusterWidgets.register(partitionsPanel);
        } else {
            InstanceType iType = InstanceType.valueOf(type);
            if (clusterWidgets != null) {
                if (InstanceType.MAP.equals(iType)) {
                    MapChartPanel mapChartPanel = new MapChartPanel(name, callBack);
                    MapStatisticsPanel mapStatisticsPanel = new MapStatisticsPanel(name, callBack);
                    MapBrowserPanel mapBrowserPanel = new MapBrowserPanel(name);
                    MapTimesPanel mapTimesPanel = new MapTimesPanel(name, callBack);
                    clusterWidgets.register(mapChartPanel, mapStatisticsPanel, mapTimesPanel, mapBrowserPanel);
                } else if (InstanceType.QUEUE.equals(iType)) {
                }
            }
        }
    }

    private void deRegisterAll() {
        for (ClusterWidgets cw : mapClusterWidgets.values()) {
            cw.deRegisterAll();
        }
    }

    public static HashMap<String, String> parseParamString(String string) {
        String[] ray = string.substring(0, string.length()).split("&");
        HashMap<String, String> map = new HashMap<String, String>();
        for (int i = 0; i < ray.length; i++) {
            String[] substrRay = ray[i].split("=");
            map.put(substrRay[0], substrRay[1]);
        }
        return map;
    }

    public void createAndAddClusterWidgets(ClusterView clusterView) {
        ClusterWidgets clusterWidgets = new ClusterWidgets(this, clusterView);
        clusterWidgets.mainPanel = mainPanel;
        mapClusterWidgets.put(clusterWidgets.clusterId, clusterWidgets);
        clusterWidgets.clusterName = clusterView.getGroupName();
        dsPanel.add(clusterWidgets.clusterTree, clusterView.getGroupName());
        setupTimer();
    }

    public synchronized void setupTimer() {
        if (refreshTimer == null) {
            refreshTimer = new RefreshTimer(this);
            refreshTimer.scheduleRepeating(REFRESH_INTERVAL);
        }
    }
}
