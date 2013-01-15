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

public class DefaultProxyFactory implements ProxyFactory {

    private final HazelcastInstanceImpl instance;

    public DefaultProxyFactory(HazelcastInstanceImpl instance) {
        super();
        this.instance = instance;
    }

//    public MProxy createMapProxy(String name) {
//        return new MProxyImpl(name, instance);
//    }

//    public QProxy createQueueProxy(String name) {
//        return new QProxyImpl(name, factory);
//    }

//    public TopicProxy createTopicProxy(String name) {
//        return new TopicProxyImpl(name, factory);
//    }
//
//    public MultiMapProxy createMultiMapProxy(String name) {
//        return new MultiMapProxyImpl(name, factory);
//    }
//
//    public ListProxy createListProxy(String name) {
//        return new ListProxyImpl(name, factory);
//    }
//
//    public SetProxy createSetProxy(String name) {
//        return new SetProxyImpl(name, factory);
//    }
//
//    public LockProxy createLockProxy(Object key) {
//        return new LockProxyImpl(factory, key);
//    }
//
//    public AtomicNumberProxy createAtomicNumberProxy(String name) {
//        return new AtomicNumberProxyImpl(name, factory);
//    }
//
//    public SemaphoreProxy createSemaphoreProxy(String name) {
//        return new SemaphoreProxyImpl(name, factory);
//    }
//
//    public CountDownLatchProxy createCountDownLatchProxy(String name) {
//        return new CountDownLatchProxyImpl(name, factory);
//    }
//
//    public IdGeneratorProxy createIdGeneratorProxy(String name) {
//        return new IdGeneratorProxyImpl(name, factory);
//    }
//
//    public ExecutorService createExecutorServiceProxy(String name) {
//        return new ExecutorServiceProxy(factory.node, name);
//    }

}
