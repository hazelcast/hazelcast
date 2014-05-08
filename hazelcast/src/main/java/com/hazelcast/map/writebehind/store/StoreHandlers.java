/*
* Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
*/

package com.hazelcast.map.writebehind.store;

import com.hazelcast.map.MapService;
import com.hazelcast.map.MapStoreWrapper;

import java.util.List;

/**
 * Static factory creating store handler chain.
 */
public final class StoreHandlers {

    private StoreHandlers() {
    }

    public static StoreHandlerChain createHandlers(MapService mapService,
                                                   MapStoreWrapper storeWrapper, List<StoreListener> storeListeners) {
        final StoreHandlerChain chain = new StoreHandlerChain(mapService, storeListeners);
        chain.register(new WriteStoreHandler(storeWrapper));
        chain.register(new DeleteStoreHandler(storeWrapper));

        return chain;
    }
}
