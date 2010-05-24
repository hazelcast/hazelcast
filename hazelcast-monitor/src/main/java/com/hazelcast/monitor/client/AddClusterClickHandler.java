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
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.TextBox;

public class AddClusterClickHandler implements ClickHandler {
    private TextBox groupName;
    private TextBox pass;
    private TextBox addresses;
    private Label lbError;
    final private HazelcastMonitor hazelcastMonitor;

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
            hazelcastService.connectCluster(groupName.getText().trim(), pass.getText().trim(),
                    addresses.getText().trim(), new AsyncCallback<ClusterView>() {
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
}
