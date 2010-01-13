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
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.*;

import java.util.Iterator;
import java.util.List;

public class AddClusterClickHandler implements ClickHandler {
    private TextBox groupName;
    private TextBox pass;
    private TextBox addresses;
    private Label lbError;
    private HazelcastMonitor hazelcastMonitor;


    /**
     * Create a remote service proxy to talk to the server-side Hazelcast
     * service.
     */
    private final HazelcastServiceAsync hazelcastService = GWT
            .create(HazelcastService.class);

    public AddClusterClickHandler(HazelcastMonitor hazelcastMonitor, TextBox groupName, TextBox pass, TextBox addresses,
                                  Label lbError) {
        this.hazelcastMonitor = hazelcastMonitor;
        this.groupName = groupName;
        this.pass = pass;
        this.addresses = addresses;
        this.lbError = lbError;
    }

    public void onClick(ClickEvent event) {
//        lbError.setText("");
        connectToCluster();
    }

    private void connectToCluster() {
        try {
            hazelcastService.connectCluster(groupName.getText(), pass.getText(), addresses.getText(), new AsyncCallback<ClusterView>() {
                public void onSuccess(ClusterView clusterView) {
                    hazelcastMonitor.createAndAddClusterWidgets(clusterView);
                }

                public void onFailure(Throwable caught) {
                    handleException(caught, lbError);
                }
            });
        } catch (ConnectionExceptoin e) {
            handleException(e, lbError);

        }
    }

    private void handleException(Throwable caught, Label error) {
        error.setText(caught.getLocalizedMessage());
        error.setVisible(true);
    }

    public static ClusterWidgets createClusterWidgets(ClusterView cv) {
        ClusterWidgets clusterWidgets = new ClusterWidgets();
        clusterWidgets.clusterId = cv.getId();
        Tree tree = new Tree();
        addTreeItems(cv, tree, clusterWidgets);
        clusterWidgets.clusterTree = tree;

        return clusterWidgets;
    }

    private static void addTreeItems(ClusterView cv, Tree tree, ClusterWidgets clusterWidgets) {
        clusterWidgets.memberTreeItem = addTreeItem(tree, "Members", cv.getMembers(), clusterWidgets.clusterId, null);
        clusterWidgets.itemMap.put(InstanceType.MAP, new InstanceWidgets(InstanceType.MAP,
                addTreeItem(tree, "Maps", cv.getMaps(), clusterWidgets.clusterId, InstanceType.MAP)));
        clusterWidgets.itemMap.put(InstanceType.QUEUE, new InstanceWidgets(InstanceType.QUEUE,
                addTreeItem(tree, "Queues", cv.getQs(), clusterWidgets.clusterId, InstanceType.QUEUE)));
        clusterWidgets.itemMap.put(InstanceType.LIST, new InstanceWidgets(InstanceType.LIST,
                addTreeItem(tree, "Lists", cv.getLists(), clusterWidgets.clusterId, InstanceType.LIST)));
        clusterWidgets.itemMap.put(InstanceType.SET, new InstanceWidgets(InstanceType.SET,
                addTreeItem(tree, "Sets", cv.getSets(), clusterWidgets.clusterId, InstanceType.SET)));
        clusterWidgets.itemMap.put(InstanceType.TOPIC, new InstanceWidgets(InstanceType.TOPIC,
                addTreeItem(tree, "Topics", cv.getTopics(), clusterWidgets.clusterId, InstanceType.TOPIC)));
        clusterWidgets.itemMap.put(InstanceType.MULTIMAP, new InstanceWidgets(InstanceType.MULTIMAP,
                addTreeItem(tree, "MultiMaps", cv.getMultiMaps(), clusterWidgets.clusterId, InstanceType.MULTIMAP)));
        clusterWidgets.itemMap.put(InstanceType.LOCK, new InstanceWidgets(InstanceType.LOCK,
                addTreeItem(tree, "Locks", cv.getLocks(), clusterWidgets.clusterId, InstanceType.LOCK)));
    }

    private static TreeItem addTreeItem(Tree tree, String headerName, List<String> itemList, int clusterId, InstanceType type) {
        TreeItem treeItem = new TreeItem(headerName);
        addItems(itemList, treeItem, clusterId, type);
        tree.addItem(treeItem);
        return treeItem;
    }

    private static void addItems(List<String> itemList, TreeItem treeItem, int clusterId, InstanceType type) {
        for (Iterator<String> iterator = itemList.iterator(); iterator.hasNext();) {
            String string = iterator.next();
            Hyperlink link = getInstanceLink(clusterId, type, string);
            treeItem.addItem(link);

        }
    }

    public static Hyperlink getInstanceLink(int clusterId, InstanceType type, String name) {
        String token =
                "clusterId=" + clusterId +
                        "&type=" + ((type == null) ? "MEMBER" : type) +
                        "&name=" + name;
        Hyperlink link = new Hyperlink(name, token);
        link.addStyleName("link");
        return link;
    }
}
