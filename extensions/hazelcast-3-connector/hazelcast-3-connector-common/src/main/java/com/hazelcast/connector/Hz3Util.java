/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.connector;

import com.hazelcast.connector.map.Hz3MapAdapter;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.jet.JetException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

final class Hz3Util {

    private Hz3Util() {
    }

    static Hz3MapAdapter createMapAdapter(String clientXml) {
        try {
            Class<?> clazz = Thread.currentThread().getContextClassLoader()
                                   .loadClass("com.hazelcast.connector.map.impl.MapAdapterImpl");
            Constructor<?> constructor = clazz.getDeclaredConstructor(String.class);
            return (Hz3MapAdapter) constructor.newInstance(clientXml);
        } catch (InvocationTargetException e) {
            throw new JetException("Could not create instance of MapAdapterImpl", e.getCause());
        } catch (InstantiationException | IllegalAccessException
                | ClassNotFoundException | NoSuchMethodException e) {
            throw new HazelcastException(e);
        }
    }
}
