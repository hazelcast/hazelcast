/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.atomic;

import com.hazelcast.atomic.proxy.AtomicNumberProxy;
import com.hazelcast.config.Config;
import com.hazelcast.spi.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

// author: sancar - 21.12.2012
public class AtomicNumberService implements ManagedService , RemoteService, MigrationAwareService{

    public static final String NAME = "hz:impl:atomicNumberService";

    private NodeEngine nodeEngine;

    private final Map<String, Long> numbers = new HashMap<String, Long>();

    private final ConcurrentMap<String, AtomicNumberProxy> proxies = new ConcurrentHashMap<String, AtomicNumberProxy>();


    public AtomicNumberService(){

    }

    public long getNumber(String name) {
        Long value = numbers.get(name);
        if(value == null){
            value = Long.valueOf(0);
            numbers.put(name,value);
        }
        return value;
    }

    public void setNumber(String name, long newValue) {
        numbers.put(name,newValue);
    }

    public void removeNumber(String name){
        numbers.remove(name);
    }

    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
    }

    public void destroy() {

    }

    public Config getConfig(){
        return nodeEngine.getConfig();
    }

    public ServiceProxy getProxy(Object... params) {
        final String name = String.valueOf(params[0]);
        if (params.length > 1 && Boolean.TRUE.equals(params[1])) {
            return new AtomicNumberProxy(name, nodeEngine);
        }
        AtomicNumberProxy proxy = proxies.get(name);
        if (proxy == null) {
            proxy = new AtomicNumberProxy(name, nodeEngine);
            final AtomicNumberProxy currentProxy = proxies.putIfAbsent(name, proxy);
            proxy = currentProxy != null ? currentProxy : proxy;
        }
        return proxy;
    }

    public Collection<ServiceProxy> getProxies() {
        return new HashSet<ServiceProxy>(proxies.values());
    }

    public void beforeMigration(MigrationServiceEvent migrationServiceEvent) {
    }

    public Operation prepareMigrationOperation(MigrationServiceEvent migrationServiceEvent) {

        return null;
    }

    public void commitMigration(MigrationServiceEvent migrationServiceEvent) {
    }

    public void rollbackMigration(MigrationServiceEvent migrationServiceEvent) {
    }

    public int getMaxBackupCount() {
        return 0;
    }
}
