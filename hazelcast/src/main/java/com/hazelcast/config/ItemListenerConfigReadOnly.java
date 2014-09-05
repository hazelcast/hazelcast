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

package com.hazelcast.config;

import com.hazelcast.core.ItemListener;

import java.util.EventListener;

/**
 * Contains the configuration for an Item Listener(Read-only).
 */
public class ItemListenerConfigReadOnly extends ItemListenerConfig {

    public ItemListenerConfigReadOnly(ItemListenerConfig config) {
        super(config);
    }

    public ItemListenerConfig setImplementation(ItemListener implementation) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public ItemListenerConfig setIncludeValue(boolean includeValue) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public ListenerConfig setClassName(String className) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public ListenerConfig setImplementation(EventListener implementation) {
        throw new UnsupportedOperationException("This config is read-only");
    }
}
