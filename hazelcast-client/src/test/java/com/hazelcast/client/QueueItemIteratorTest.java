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

package com.hazelcast.client;

import com.hazelcast.client.proxy.QueueClientProxy;
import com.hazelcast.client.util.QueueItemIterator;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.NoSuchElementException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class QueueItemIteratorTest {

    @Test(expected = NoSuchElementException.class)
    public void emtpyIterator() {
        QueueItemIterator<String> it = new QueueItemIterator(new String[]{}, null);
        assertFalse(it.hasNext());
        it.next();
    }

    @Test(expected = IllegalStateException.class)
    public void removeOnEmptyIterator() {
        QueueItemIterator<String> it = new QueueItemIterator(new String[]{}, null);
        assertFalse(it.hasNext());
        it.remove();
    }

    @Test(expected = IllegalStateException.class)
    public void callremoveTwice() {
        QueueClientProxy mockQCP = mock(QueueClientProxy.class);
        QueueItemIterator<String> it = new QueueItemIterator(new String[]{"a"}, mockQCP);
        assertTrue(it.hasNext());
        String v = it.next();
        assertEquals("a", v);
        it.remove();
        verify(mockQCP).remove("a");
        it.remove();
    }

    @Test
    public void fivelementsIterate() {
        QueueClientProxy mockQCP = mock(QueueClientProxy.class);
        QueueItemIterator<String> it = new QueueItemIterator(new String[]{"1", "2", "3", "4", "5"}, mockQCP);
        int counter = 0;
        while (it.hasNext()) {
            String v = it.next();
            it.remove();
            counter++;
        }
        assertEquals(5, counter);
        verify(mockQCP).remove("1");
        verify(mockQCP).remove("2");
        verify(mockQCP).remove("3");
        verify(mockQCP).remove("4");
        verify(mockQCP).remove("5");
    }
}

