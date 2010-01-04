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

import com.hazelcast.core.IQueue;
import com.hazelcast.core.ItemListener;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

interface QProxy extends IQueue {

    boolean offer(Object obj);

    boolean offer(Object obj, long timeout, TimeUnit unit) throws InterruptedException;

    void put(Object obj) throws InterruptedException;

    Object peek();

    Object poll();

    Object poll(long timeout, TimeUnit unit) throws InterruptedException;

    Object take() throws InterruptedException;

    int remainingCapacity();

    Iterator iterator();

    int size();

    void addItemListener(ItemListener listener, boolean includeValue);

    void removeItemListener(ItemListener listener);

    String getName();

    boolean remove(Object obj);

    int drainTo(Collection c);

    int drainTo(Collection c, int maxElements);

    void destroy();

    InstanceType getInstanceType();
}
