/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl;

import java.util.concurrent.atomic.AtomicLong;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Prefix;
import com.hazelcast.impl.base.FactoryAwareNamedProxy;
import com.hazelcast.nio.DataSerializable;

public class IdGeneratorProxyImpl extends FactoryAwareNamedProxy implements IdGeneratorProxy, DataSerializable {

    private transient IdGeneratorProxy base = null;

    public IdGeneratorProxyImpl() {
    }

    public IdGeneratorProxyImpl(String name, FactoryImpl factory) {
        setName(name);
        setHazelcastInstance(factory);
        base = new IdGeneratorBase();
    }

    private void ensure() {
        factory.initialChecks();
        if (base == null) {
            base = (IdGeneratorProxy) factory.getOrCreateProxyByName(name);
        }
    }

    public Object getId() {
        ensure();
        return base.getId();
    }

    @Override
    public String toString() {
        return "IdGenerator [" + getName() + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IdGeneratorProxyImpl that = (IdGeneratorProxyImpl) o;
        return !(name != null ? !name.equals(that.name) : that.name != null);
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }

    public InstanceType getInstanceType() {
        ensure();
        return base.getInstanceType();
    }

    public void destroy() {
        ensure();
        base.destroy();
    }

    public String getName() {
        ensure();
        return base.getName();
    }

    public long newId() {
        ensure();
        return base.newId();
    }

    private class IdGeneratorBase implements IdGeneratorProxy {

        private static final long MILLION = 1000L * 1000L;

        final AtomicLong million = new AtomicLong(-1);

        final AtomicLong currentId = new AtomicLong(2 * MILLION);

        public String getName() {
            return name.substring(Prefix.IDGEN.length());
        }

        public long newId() {
            long idAddition = currentId.incrementAndGet();
            if (idAddition >= MILLION) {
                synchronized (IdGeneratorBase.this) {
                    idAddition = currentId.get();
                    if (idAddition >= MILLION) {
                        Long idMillion = getNewMillion();
                        long newMillion = idMillion * MILLION;
                        million.set(newMillion);
                        currentId.set(0L);
                    }
                    return newId();
                }
            }
            long millionNow = million.get();
            return millionNow + idAddition;
        }

        private Long getNewMillion() {
            return factory.getAtomicNumber(Prefix.HAZELCAST + "idGen_" + getName()).incrementAndGet() - 1;
        }

        public InstanceType getInstanceType() {
            return InstanceType.ID_GENERATOR;
        }

        public void destroy() {
            currentId.set(2 * MILLION);
            synchronized (IdGeneratorBase.this) {
                factory.destroyInstanceClusterWide(name, null);
                factory.getAtomicNumber(Prefix.HAZELCAST + "idGen_" + getName()).destroy();
                currentId.set(2 * MILLION);
                million.set(-1);
            }
        }

        public Object getId() {
            return name;
        }
        
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
		}
    }
}
