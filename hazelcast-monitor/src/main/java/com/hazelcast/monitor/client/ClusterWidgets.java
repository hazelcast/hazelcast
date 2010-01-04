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

import com.google.gwt.user.client.ui.*;
import com.hazelcast.monitor.client.event.*;
import com.hazelcast.monitor.client.handler.InstanceCreatedHandler;
import com.hazelcast.monitor.client.handler.InstanceDestroyedHandler;
import com.hazelcast.monitor.client.handler.MemberEventHandler;

import java.util.*;

public class ClusterWidgets {
    int clusterId;
    Tree clusterTree;
    String clusterName;
    TreeItem memberTreeItem;
    public HorizontalSplitPanel mainPanel;
    Map<ChangeEventType, Widget> activeWidgets = new HashMap<ChangeEventType, Widget>();
    Map<InstanceType, InstanceWidgets> itemMap = new HashMap<InstanceType, InstanceWidgets>();
    Map<ChangeEventType, List<MonitoringPanel>> panels = new HashMap<ChangeEventType, List<MonitoringPanel>>();

    public Map<ChangeEventType, List<MonitoringPanel>> getPanels() {
        return panels;
    }


    public Map<ChangeEventType, Widget> getActiveWidgets() {
        return activeWidgets;
    }

    public TreeItem getMemberTreeItem() {
        return memberTreeItem;
    }

    public Map<InstanceType, InstanceWidgets> getItemMap() {
        return itemMap;
    }

    public void handle(ChangeEvent changeEvent) {
        System.out.println("Handling event: " + changeEvent);
        if (changeEvent instanceof InstanceCreated) {
            new InstanceCreatedHandler(this).handle(changeEvent);
        } else if (changeEvent instanceof InstanceDestroyed) {
            new InstanceDestroyedHandler(this).handle(changeEvent);
        } else if (changeEvent instanceof MemberEvent) {
            new MemberEventHandler(this).handle(changeEvent);
        } else {
            List<MonitoringPanel> list = panels.get(changeEvent.getChangeEventType());

            if (list == null || list.isEmpty()) {
                System.out.println("Unknown event:" + changeEvent.getChangeEventType());
                return;
            }
            for (Iterator<MonitoringPanel> iterator = list.iterator(); iterator.hasNext();) {
                MonitoringPanel monitoringPanel = iterator.next();
                monitoringPanel.handle(changeEvent);
            }
        }
    }


    public void register(MonitoringPanel panel) {
        boolean registered = panel.register(this);
        if (registered) {
            VerticalPanel rightPanel = (VerticalPanel) mainPanel.getRightWidget();
            rightPanel.add(panel.getDsp());
        }
    }

    public void deRegister(MonitoringPanel panel) {
        panel.deRegister(this);
        VerticalPanel rightPanel = (VerticalPanel) mainPanel.getRightWidget();
        rightPanel.remove(panel.getDsp());
    }

    public void deRegisterAll() {
        Set<ChangeEventType> s = panels.keySet();
        for (Iterator<ChangeEventType> iterator = s.iterator(); iterator.hasNext();) {
            ChangeEventType key = iterator.next();
            List<MonitoringPanel> list = panels.get(key);
            while (list.size() > 0) {
                MonitoringPanel panel = list.get(0);
                deRegister(panel);
            }
            panels.remove(key);
        }
    }
}
