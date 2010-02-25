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

import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.DisclosurePanel;
import com.hazelcast.monitor.client.event.ChangeEvent;

public class MapPageBuilder implements PageBuilder {

    public void buildPage(ClusterWidgets clusterWidgets, String name, AsyncCallback<ChangeEvent> callBack, ServicesFactory servicesFactory) {
        MapChartPanel mapChartPanel = new MapChartPanel(name, callBack, servicesFactory);
        ((DisclosurePanel)mapChartPanel.getPanelWidget()).setOpen(true);
        MapThroughputPanel mapThroughputPanel = new MapThroughputPanel(name, callBack, servicesFactory);
        ((DisclosurePanel)mapThroughputPanel.getPanelWidget()).setOpen(true);
        MapEntryOwnerShipPanel mapStatisticsPanel = new MapEntryOwnerShipPanel(name, callBack, servicesFactory);
        MapBrowserPanel mapBrowserPanel = new MapBrowserPanel(name, callBack, servicesFactory);
        MapTimesPanel mapTimesPanel = new MapTimesPanel(name, callBack, servicesFactory);
        clusterWidgets.register(mapChartPanel, mapThroughputPanel, mapStatisticsPanel, mapTimesPanel, mapBrowserPanel);
    }
}
