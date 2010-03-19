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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Entry point classes define <code>onModuleLoad()</code>.
 */
public class HazelcastMonitor implements EntryPoint, ValueChangeHandler {
    private static final String LEFT_PANEL_SIZE = "230";
    private static final int REFRESH_INTERVAL = 1000;
    Map<Integer, ClusterWidgets> mapClusterWidgets = new HashMap<Integer, ClusterWidgets>();
    OnValueChangeHandler onValueChangeHandler;
    HorizontalSplitPanel mainPanel;
    DecoratedStackPanel dsPanel;
    DisclosurePanel clusterAddPanel;
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
        clusterAddPanel = createClusterAddPanel();
        leftPanel.add(clusterAddPanel);
        dsPanel = new DecoratedStackPanel();
        dsPanel.setWidth(LEFT_PANEL_SIZE);
        leftPanel.add(dsPanel);
        mainPanel.setLeftWidget(leftPanel);
        VerticalPanel rightPanel = new VerticalPanel();
        mainPanel.setRightWidget(rightPanel);
        RootPanel.get().add(mainPanel);
        History.addValueChangeHandler(this);
        loadActiveClusterViews();
        ServicesFactory servicesFactory = new ServicesFactory();
        onValueChangeHandler = new OnValueChangeHandler(servicesFactory, this);
    }

    void closeClusterAddPanel() {
        clusterAddPanel.setOpen(false);
    }

    void addToRightPanel(Widget widget) {
        VerticalPanel rightPanel = (VerticalPanel) mainPanel.getRightWidget();
        rightPanel.add(widget);
    }

    private void loadActiveClusterViews() {
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

    public void onValueChange(final ValueChangeEvent event) {
        String token = event.getValue().toString();
        onValueChangeHandler.handle(token);
    }

    public void createAndAddClusterWidgets(ClusterView clusterView) {
        ClusterWidgets clusterWidgets = new ClusterWidgets(this, clusterView);
        clusterWidgets.mainPanel = mainPanel;
        mapClusterWidgets.put(clusterWidgets.clusterId, clusterWidgets);
        clusterWidgets.clusterName = clusterView.getGroupName();
        dsPanel.add(clusterWidgets.getClusterTree(), clusterView.getGroupName());
        setupTimer();
    }

    public Map<Integer, ClusterWidgets> getMapClusterWidgets() {
        return mapClusterWidgets;
    }

    private synchronized void setupTimer() {
        if (refreshTimer == null) {
            refreshTimer = new RefreshTimer(this);
            refreshTimer.scheduleRepeating(REFRESH_INTERVAL);
        }
    }

    private DisclosurePanel createClusterAddPanel() {
        final DisclosurePanel disclosurePanel = new DisclosurePanel(
                "Add Cluster to Monitor");
        final TextBox tbGroupName = new TextBox();
        tbGroupName.setText("dev");
        tbGroupName.setWidth("100px");
        final TextBox tbGroupPass = new PasswordTextBox();
        tbGroupPass.setText("dev-pass");
        tbGroupPass.setWidth("100px");
        final TextBox tbAddresses = new TextBox();
        tbAddresses.setWidth("100px");
//        tbAddresses.setText("192.168.1.3");
        final Label lbError = new Label("");
        lbError.setVisible(false);
        final Button btAddCluster = new Button("Add Cluster");
        btAddCluster.addClickHandler(new AddClusterClickHandler(this, tbGroupName, tbGroupPass, tbAddresses, lbError));
        FlexTable table = new FlexTable();
        table.setWidget(0,0,new Label("Group Name:"));
        table.setWidget(1,0,new Label("Password:"));
        table.setWidget(2,0,new Label("Address:"));
        table.setWidget(0,1,tbGroupName);
        table.setWidget(1,1,tbGroupPass);
        table.setWidget(2,1,tbAddresses);
        table.setWidget(3,1, btAddCluster);
        table.setWidget(4,0, lbError);
        table.getFlexCellFormatter().setColSpan(4,0,2);
        table.getCellFormatter().setHorizontalAlignment(0,0, HasHorizontalAlignment.ALIGN_RIGHT);
        table.getCellFormatter().setHorizontalAlignment(1,0, HasHorizontalAlignment.ALIGN_RIGHT);
        table.getCellFormatter().setHorizontalAlignment(2,0, HasHorizontalAlignment.ALIGN_RIGHT);
//        disclosurePanel.add(vPanel);
        disclosurePanel.add(table);
        disclosurePanel.setOpen(true);
        return disclosurePanel;

    }
}
