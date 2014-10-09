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

package com.hazelcast.instance;

import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.ServiceLoader;

import java.util.Iterator;

public final class NodeExtensionFactory {

    private static final String FACTORY_ID = "com.hazelcast.instance.NodeExtension";

    private NodeExtensionFactory() {
    }

    public static NodeExtension create(ClassLoader classLoader) {
        try {
            Iterator<NodeExtension> iter = ServiceLoader.iterator(NodeExtension.class, FACTORY_ID, classLoader);
            while (iter.hasNext()) {
                NodeExtension initializer = iter.next();
                if (!(initializer.getClass().equals(DefaultNodeExtension.class))) {
                    return initializer;
                }
            }
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        return new DefaultNodeExtension();
    }
}
