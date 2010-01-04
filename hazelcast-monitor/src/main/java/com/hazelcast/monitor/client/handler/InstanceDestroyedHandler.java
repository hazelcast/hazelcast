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
package com.hazelcast.monitor.client.handler;

import com.google.gwt.user.client.ui.Hyperlink;
import com.google.gwt.user.client.ui.TreeItem;
import com.hazelcast.monitor.client.ClusterWidgets;
import com.hazelcast.monitor.client.InstanceWidgets;
import com.hazelcast.monitor.client.event.ChangeEvent;
import com.hazelcast.monitor.client.event.InstanceDestroyed;

public class InstanceDestroyedHandler implements ChangeEventHandler {
    private ClusterWidgets clusterWidgets;

    public InstanceDestroyedHandler() {
    }

    public InstanceDestroyedHandler(ClusterWidgets clusterWidgets) {
        this.clusterWidgets = clusterWidgets;

    }

    public void handle(ChangeEvent e) {
        InstanceDestroyed event = (InstanceDestroyed) e;
        InstanceWidgets instanceWidgets = clusterWidgets.getItemMap().get(event.getInstanceType());
        String name = event.getName();
        if (name == null) {
            return;
        }
        for (int i = 0; i < instanceWidgets.getTreeItem().getChildCount(); i++) {
            TreeItem item = instanceWidgets.getTreeItem().getChild(i);
            Hyperlink link = (Hyperlink) item.getWidget();
            if (name.equals(link.getText())) {
                System.out.println("Removing instance: " + event.getInstanceType() + ", " + name);
                instanceWidgets.getTreeItem().removeItem(item);
            }
        }
    }
}
