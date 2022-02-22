/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl;

import com.hazelcast.map.listener.MapListener;

import static com.hazelcast.map.impl.MapListenerAdaptors.createMapListenerAdaptor;

/**
 * Contains support methods for creating various {@link com.hazelcast.map.impl.ListenerAdapter ListenerAdapter}
 *
 * @see com.hazelcast.map.impl.MapListenerAdaptors
 */
public final class ListenerAdapters {

    private ListenerAdapters() {
    }

    public static <T> ListenerAdapter<T> createListenerAdapter(Object listener) {
        if (listener instanceof ListenerAdapter) {
            return ((ListenerAdapter<T>) listener);
        }

        if (listener instanceof MapListener) {
            return createMapListenerAdaptor((MapListener) listener);
        }

        throw new IllegalArgumentException("Not a valid type to create a listener: " + listener.getClass().getSimpleName());
    }
}
