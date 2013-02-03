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

package com.hazelcast.impl.base;

import com.hazelcast.impl.Node;
import com.hazelcast.impl.ProxyFactory;
import com.hazelcast.impl.concurrentmap.RecordFactory;
import com.hazelcast.security.SecurityContext;

public interface NodeInitializer {

    void beforeInitialize(Node node);

    void printNodeInfo(Node node);

    void afterInitialize(Node node);

    String getBuild();

    int getBuildNumber();

    String getVersion();

    ProxyFactory getProxyFactory();

    RecordFactory getRecordFactory();

    SecurityContext getSecurityContext();

    void destroy();
}
