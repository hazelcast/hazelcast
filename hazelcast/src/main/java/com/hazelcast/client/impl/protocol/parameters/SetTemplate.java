/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.parameters;

import com.hazelcast.annotation.EncodeMethod;
import com.hazelcast.annotation.GenerateParameters;
import com.hazelcast.nio.serialization.Data;

import java.util.List;
import java.util.Set;

@GenerateParameters(id = TemplateConstants.SET_TEMPLATE_ID, name = "Set", ns = "Hazelcast.Client.Protocol.Set")
public interface SetTemplate {

    @EncodeMethod(id = 1)
    void size(String name);

    @EncodeMethod(id = 2)
    void contains(String name, Data value);

    @EncodeMethod(id = 3)
    void containsAll(String name, Set<Data> valueSet);

    @EncodeMethod(id = 4)
    void add(String name, Data value);

    @EncodeMethod(id = 5)
    void remove(String name, Data value);

    @EncodeMethod(id = 6)
    void addAll(String name, List<Data> valueList);

    @EncodeMethod(id = 7)
    void compareAndRemoveAll(String name, Set<Data> valueSet);

    @EncodeMethod(id = 8)
    void compareAndRetainAll(String name, Set<Data> valueSet);

    @EncodeMethod(id = 9)
    void clear(String name);

    @EncodeMethod(id = 10)
    void getAll(String name);

    @EncodeMethod(id = 11)
    void addListener(String name, boolean includeValue);

    @EncodeMethod(id = 12)
    void removeListener(String name, String registrationId);

    @EncodeMethod(id = 13)
    void isEmpty(String name);

}
