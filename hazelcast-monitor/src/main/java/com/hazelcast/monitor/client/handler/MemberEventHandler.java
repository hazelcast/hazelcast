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

import com.google.gwt.user.client.ui.Anchor;
import com.google.gwt.user.client.ui.TreeItem;
import com.hazelcast.monitor.client.ClusterWidgets;
import com.hazelcast.monitor.client.event.ChangeEvent;
import com.hazelcast.monitor.client.event.MemberEvent;

import java.util.Iterator;
import java.util.List;

public class MemberEventHandler implements ChangeEventHandler {
    final private ClusterWidgets clusterWidgets;

    public MemberEventHandler(ClusterWidgets clusterWidgets) {
        this.clusterWidgets = clusterWidgets;
    }

    public void handle(ChangeEvent event) {
        if (!(event instanceof MemberEvent)) {
            return;
        }
        MemberEvent memberEvent = (MemberEvent) event;
        List<String> currentMembers = memberEvent.getMembers();
        int memberSize = memberEvent.getMembers().size();
        TreeItem memberTreeItem = clusterWidgets.getMemberTreeItem();
        for (int i = 0; i < memberTreeItem.getChildCount(); i++) {
            TreeItem treeItem = memberTreeItem.getChild(i);
            Anchor link = (Anchor) treeItem.getWidget();
            String member = link.getText();
            if (!currentMembers.contains(member)) {
                memberTreeItem.removeItem(treeItem);
            } else {
                currentMembers.remove(member);
            }
        }
        addNewMembers(currentMembers, memberTreeItem);
        String headerText = "Members (" + memberSize + ")";
        clusterWidgets.getMemberTreeItem().setText(headerText);
    }

    private void addNewMembers(List<String> currentMembers, TreeItem memberTreeItem) {
        for (Iterator<String> iterator = currentMembers.iterator(); iterator.hasNext();) {
            String string = iterator.next();
            Anchor link = clusterWidgets.getInstanceLink(null, string);
            memberTreeItem.addItem(link);
        }
    }
}
