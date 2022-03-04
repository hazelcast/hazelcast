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

package com.hazelcast.scheduledexecutor.impl;

import com.hazelcast.core.ManagedContext;
import com.hazelcast.partition.PartitionAware;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.scheduledexecutor.NamedTask;
import com.hazelcast.scheduledexecutor.StatefulTask;
import com.hazelcast.internal.services.NodeAware;

import java.util.Map;

public class ScheduledRunnableAdapter<V> extends AbstractTaskDecorator<V>
        implements NodeAware, PartitionAware, NamedTask, StatefulTask {

    public ScheduledRunnableAdapter() {
    }

    public ScheduledRunnableAdapter(Runnable task) {
        super(task);
    }

    public Runnable getRunnable() {
        return (Runnable) delegate;
    }

    public void setRunnable(Runnable runnable) {
        delegate = runnable;
    }

    @Override
    public V call()
            throws Exception {
        ((Runnable) delegate).run();
        return null;
    }

    @Override
    public Object getPartitionKey() {
        if (delegate instanceof PartitionAware) {
            return ((PartitionAware) delegate).getPartitionKey();
        }
        return null;
    }

    @Override
    public void setNode(Node node) {
        ManagedContext managedContext = node.getSerializationService().getManagedContext();
        delegate = managedContext.initialize(delegate);
    }

    @Override
    public String getName() {
        if (delegate instanceof NamedTask) {
            return ((NamedTask) delegate).getName();
        }
        return null;
    }

    @Override
    public void save(Map snapshot) {
        if (delegate instanceof StatefulTask) {
            ((StatefulTask) delegate).save(snapshot);
        }
    }

    @Override
    public void load(Map snapshot) {
        if (delegate instanceof StatefulTask) {
            ((StatefulTask) delegate).load(snapshot);
        }
    }

    @Override
    public int getClassId() {
        return ScheduledExecutorDataSerializerHook.RUNNABLE_ADAPTER;
    }

    @Override
    public String toString() {
        return "ScheduledRunnableAdapter" + "{task=" + delegate + '}';
    }

}
