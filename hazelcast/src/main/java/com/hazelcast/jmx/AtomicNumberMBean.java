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

package com.hazelcast.jmx;

import com.hazelcast.core.AtomicNumber;

/**
 * MBean for AtomicNumber
 *
 * @author Vladimir Dolzhenko, vladimir.dolzhenko@gmail.com
 */
@JMXDescription("A distributed atomic number")
public class AtomicNumberMBean extends AbstractMBean<AtomicNumber> {

    public AtomicNumberMBean(AtomicNumber managedObject, ManagementService managementService) {
        super(managedObject, managementService);
    }

    @Override
    public ObjectNameSpec getNameSpec() {
        return getParentName().getNested("AtomicNumber", getName());
    }

    @JMXAttribute("ActualValue")
    @JMXDescription("get() result")
    public long getActualValue() {
        return getManagedObject().get();
    }

    @JMXAttribute("Name")
    @JMXDescription("Registration name of the atomic number")
    public String getName() {
        return getManagedObject().getName();
    }

    @JMXOperation("set")
    @JMXDescription("set value")
    public void set(final long newValue){
        getManagedObject().set(newValue);
    }
    
    @JMXOperation("add")
    @JMXDescription("add value")
    public void add(final long delta){
        getManagedObject().addAndGet(delta);
    }
    
    @JMXOperation("reset")
    @JMXDescription("reset value")
    public void reset(){
        getManagedObject().set(0L);
    }

    @JMXOperation("compareAndSet")
    @JMXDescription("compareAndSet value")
    public void compareAndSet(final long expectedValue, final long newValue){
        getManagedObject().compareAndSet(expectedValue, newValue);
    }
}
