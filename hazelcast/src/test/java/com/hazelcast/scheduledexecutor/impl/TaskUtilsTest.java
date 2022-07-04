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

import com.hazelcast.scheduledexecutor.AutoDisposableTask;
import com.hazelcast.scheduledexecutor.NamedTask;
import com.hazelcast.scheduledexecutor.TaskUtils;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.scheduledexecutor.TaskUtils.autoDisposable;
import static com.hazelcast.scheduledexecutor.TaskUtils.named;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TaskUtilsTest extends ScheduledExecutorServiceTestSupport {

    @Test
    public void testConstructor() {
        assertUtilityConstructor(TaskUtils.class);
    }

    @Test
    public void decoratedTask_whenPlainCallableTask() {
        String taskName = "Name 1";
        String taskName2 = "Name 2";
        PlainCallableTask plainCallableTask = new PlainCallableTask();

        AbstractTaskDecorator<?> decoratedTask = (AbstractTaskDecorator) autoDisposable(named(taskName, named(taskName2, autoDisposable(autoDisposable(plainCallableTask)))));

        assertTrue(decoratedTask.isDecoratedWith(NamedTask.class));
        assertTrue(decoratedTask.isDecoratedWith(AutoDisposableTask.class));
        assertEquals(taskName, decoratedTask.undecorateTo(NamedTask.class).getName());
    }

    @Test
    public void decoratedTask_whenCallableImplementingAutoDisposableTask() {
        String taskName = "Name 1";
        AutoDisposableCallable autoDisposableTask = new AutoDisposableCallable();

        AbstractTaskDecorator<?> decoratedTask = (AbstractTaskDecorator) named(taskName, autoDisposableTask);

        assertTrue(decoratedTask.isDecoratedWith(NamedTask.class));
        assertEquals(taskName, decoratedTask.undecorateTo(NamedTask.class).getName());
        assertTrue(decoratedTask.isDecoratedWith(AutoDisposableTask.class));
    }

    @Test
    public void decoratedTask_whenCallableImplementingNamedTask() {
        NamedCallable namedTaskCallable = new NamedCallable();

        AbstractTaskDecorator<?> decoratedTask = (AbstractTaskDecorator) autoDisposable(namedTaskCallable);

        assertTrue(decoratedTask.isDecoratedWith(AutoDisposableTask.class));
        assertTrue(decoratedTask.isDecoratedWith(NamedTask.class));
        assertEquals(NamedCallable.NAME, decoratedTask.undecorateTo(NamedTask.class).getName());
    }

    @Test
    public void decoratedTask_whenPlainRunnableTask() {
        String taskName = "Name 1";
        String taskName2 = "Name 2";
        PlainRunnableTask plainRunnableTask = new PlainRunnableTask();

        AbstractTaskDecorator<?> decoratedTask = (AbstractTaskDecorator) autoDisposable(named(taskName, named(taskName2, autoDisposable(autoDisposable(plainRunnableTask)))));

        assertTrue(decoratedTask.isDecoratedWith(NamedTask.class));
        assertTrue(decoratedTask.isDecoratedWith(AutoDisposableTask.class));
        assertEquals(taskName, decoratedTask.undecorateTo(NamedTask.class).getName());
    }

    @Test
    public void decoratedTask_whenRunnableImplementingAutoDisposableRunnableTask() {
        String taskName = "Name 2";
        AutoDisposableRunnable autoDisposableRunnable = new AutoDisposableRunnable();

        AbstractTaskDecorator<?> decoratedTask = (AbstractTaskDecorator) named(taskName, autoDisposableRunnable);

        assertTrue(decoratedTask.isDecoratedWith(NamedTask.class));
        assertEquals(taskName, decoratedTask.undecorateTo(NamedTask.class).getName());
        assertTrue(decoratedTask.isDecoratedWith(AutoDisposableTask.class));
    }

    @Test
    public void decoratedTask_whenRunnableImplementingNamedTask() {
        NamedRunnable namedTaskRunnable = new NamedRunnable();

        AbstractTaskDecorator<?> decoratedTask = (AbstractTaskDecorator) autoDisposable(namedTaskRunnable);

        assertTrue(decoratedTask.isDecoratedWith(AutoDisposableTask.class));
        assertTrue(decoratedTask.isDecoratedWith(NamedTask.class));
        assertEquals(NamedRunnable.NAME, decoratedTask.undecorateTo(NamedTask.class).getName());
    }
}
