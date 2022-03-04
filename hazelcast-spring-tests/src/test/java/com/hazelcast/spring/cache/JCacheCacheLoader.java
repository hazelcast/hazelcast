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

package com.hazelcast.spring.cache;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.services.NodeAware;
import com.hazelcast.spring.context.SpringAware;

import javax.annotation.Resource;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

@SpringAware
public class JCacheCacheLoader
        implements CacheLoader<Object, String>, HazelcastInstanceAware, NodeAware {

    public static final AtomicBoolean HAZELCAST_INSTANCE_INJECTED = new AtomicBoolean();
    public static final AtomicBoolean NODE_INJECTED = new AtomicBoolean();

    public static JCacheCacheLoader instance;

    @Resource(name = "dummy")
    private IJCacheDummyBean dummyBean;

    public JCacheCacheLoader() {
        instance = this;
    }

    @Override
    public String load(Object key)
            throws CacheLoaderException {
        return key.toString();
    }

    @Override
    public Map<Object, String> loadAll(Iterable keys)
            throws CacheLoaderException {
        return new HashMap<>();
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        HAZELCAST_INSTANCE_INJECTED.set(true);
    }

    @Override
    public void setNode(Node node) {
        NODE_INJECTED.set(true);
    }

    public IJCacheDummyBean getDummyBean() {
        return dummyBean;
    }

    public void setDummyBean(IJCacheDummyBean dummyBean) {
        this.dummyBean = dummyBean;
    }
}
