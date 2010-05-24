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
import com.google.gwt.user.client.Timer;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.ListBox;
import com.hazelcast.monitor.client.event.ChangeEvent;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

public class
        RefreshTimer extends Timer {

    private HazelcastMonitor hazelcastMonitor;
    /**
     * Create a remote service proxy to talk to the server-side Hazelcast
     * service.
     */
    private final HazelcastServiceAsync hazelcastService = GWT
            .create(HazelcastService.class);

    public RefreshTimer(HazelcastMonitor hazelcastMonitor) {
        this.hazelcastMonitor = hazelcastMonitor;
    }

    @Override
    public void run() {
        hazelcastService.getChange(new AsyncCallback<ArrayList<ChangeEvent>>() {
            public void onFailure(Throwable caught) {
            }

            public void onSuccess(ArrayList<ChangeEvent> result) {
                for (Iterator<ChangeEvent> iterator = result.iterator(); iterator.hasNext();) {
                    ChangeEvent changeEvent = iterator.next();
                    int clusterId = changeEvent.getClusterId();
                    ClusterWidgets clusterWidgets = hazelcastMonitor.mapClusterWidgets.get(clusterId);
                    if (clusterWidgets == null) {
                        Integer[] arr = new Integer[hazelcastMonitor.mapClusterWidgets.keySet().size()];
                        arr = hazelcastMonitor.mapClusterWidgets.keySet().toArray(arr);
                        String str = "[";
                        for (int i = 0; i < arr.length; i++) {
                            str += arr[i] + ",";
                        }
                        str = str.substring(0, str.length() - 1);
                        str += "]";
                        logEvent("Event for Cluster id:" + clusterId + ". Only following clusters are active on browser:" + str);
                        return;
                    }
                    logEvent(changeEvent.toString());
                    clusterWidgets.handle(changeEvent);
                }
            }

            private void logEvent(String message) {
                ListBox logBox = hazelcastMonitor.logBox;
                logBox.addItem(new Date() + ": " + message);
                while (logBox.getItemCount() > 30) {
                    logBox.removeItem(0);
                }
                logBox.setSelectedIndex(logBox.getItemCount() - 1);
            }
        });
    }
}
