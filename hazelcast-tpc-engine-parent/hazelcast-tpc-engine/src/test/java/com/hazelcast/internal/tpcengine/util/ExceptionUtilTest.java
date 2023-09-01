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

package com.hazelcast.internal.tpcengine.util;

import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;

import static com.hazelcast.internal.tpcengine.TpcTestSupport.assertInstanceOf;
import static com.hazelcast.internal.tpcengine.util.ExceptionUtil.ignore;
import static com.hazelcast.internal.tpcengine.util.ExceptionUtil.newUncheckedIOException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class ExceptionUtilTest {

    @Test
    public void test_newUncheckedIOException() {
        String msg = "foo";
        UncheckedIOException e1 = newUncheckedIOException(msg);
        assertEquals(msg, e1.getMessage());
        assertInstanceOf(IOException.class, e1.getCause());

        IOException cause = new IOException();
        UncheckedIOException e2 = newUncheckedIOException(msg, cause);
        assertEquals(msg, e2.getMessage());
        assertSame(cause, e2.getCause());

        RuntimeException throwable = new RuntimeException();
        UncheckedIOException e3 = newUncheckedIOException(msg, throwable);
        assertEquals(msg, e3.getMessage());
        assertInstanceOf(IOException.class, e3.getCause());
        assertSame(throwable, e3.getCause().getCause());
    }

    @Test
    public void test_ignore() {
        ignore(new Exception());
    }

    @Test
    public void test_ignore_whenNull() {
        ignore(null);
    }
}
