/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine;

import com.hazelcast.internal.tpcengine.util.Reference;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultTaskRunnerTest {

    @Test
    public void test_run_whenTaskNotRunnable() {
        DefaultTaskRunner runner = new DefaultTaskRunner();
        assertThrows(ClassCastException.class, () -> runner.run(new Reference("banana")));
    }

    @Test
    public void test_run_whenTask() throws Throwable {
        DefaultTaskRunner runner = new DefaultTaskRunner();
        Task task = mock(Task.class);
        when(task.run()).thenReturn(Task.RUN_YIELD);

        int runResult = runner.run(new Reference(task));
        assertEquals(Task.RUN_YIELD, runResult);
    }

    @Test
    public void test_run_whenRunnable_runsFine() throws Throwable {
        DefaultTaskRunner runner = new DefaultTaskRunner();

        Runnable task = mock(Runnable.class);
        int result = runner.run(new Reference(task));
        assertEquals(Task.RUN_COMPLETED, result);
    }

    @Test
    public void test_run_whenRunnable_throwsException() {
        DefaultTaskRunner runner = new DefaultTaskRunner();

        Runnable task = mock(Runnable.class);
        doThrow(new StackOverflowError()).when(task).run();

        assertThrows(StackOverflowError.class, () -> runner.run(new Reference(task)));
    }

    @Test
    public void test_handleError_whenNotException() {
        DefaultTaskRunner runner = new DefaultTaskRunner();
        assertThrows(Error.class, () -> runner.handleError(this, new Error()));
    }

    @Test
    public void test_handleError_whenException() {
        DefaultTaskRunner runner = new DefaultTaskRunner();
        int runState = runner.handleError(this, new IndexOutOfBoundsException());
        assertEquals(Task.RUN_COMPLETED, runState);
    }
}
