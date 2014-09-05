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

import com.hazelcast.core.EntryListener;

import java.util.EventListener;

/**
 * Configuration for EntryListener(Read Only)
 */
public class EntryListenerConfigReadOnly extends EntryListenerConfig {

    public EntryListenerConfigReadOnly(EntryListenerConfig config) {
        super(config);
    }

    public EntryListenerConfig setImplementation(EntryListener implementation) {
        throw new UnsupportedOperationException("this config is read-only");
    }

    public EntryListenerConfig setLocal(boolean local) {
        throw new UnsupportedOperationException("this config is read-only");
    }

    public EntryListenerConfig setIncludeValue(boolean includeValue) {
        throw new UnsupportedOperationException("this config is read-only");
    }

    public ListenerConfig setClassName(String className) {
        throw new UnsupportedOperationException("this config is read-only");
    }

    public ListenerConfig setImplementation(EventListener implementation) {
        throw new UnsupportedOperationException("this config is read-only");
    }
}
