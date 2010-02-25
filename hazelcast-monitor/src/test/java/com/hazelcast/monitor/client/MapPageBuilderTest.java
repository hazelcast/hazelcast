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
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class MapPageBuilderTest {

    @Test
    public void testBuildPage() {
        MapPageBuilder mapPageBuilder = new MapPageBuilder();
        ClusterWidgets clusterWidgets = mock(ClusterWidgets.class);
        AsyncCallback callback = mock(AsyncCallback.class);
        ServicesFactory servicesFactory = mock(ServicesFactory.class);
        mapPageBuilder.buildPage(clusterWidgets, "myMap", callback, servicesFactory);
        verify(clusterWidgets, times(1)).register(any(MapChartPanel.class), any(MapEntryOwnerShipPanel.class),
                any(MapBrowserPanel.class), any(MapTimesPanel.class), any(MapThroughputPanel.class));
    }
}
