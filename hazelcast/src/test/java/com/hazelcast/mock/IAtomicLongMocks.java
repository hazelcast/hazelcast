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

package com.hazelcast.mock;

import com.hazelcast.cp.IAtomicLong;

import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.mock.MockUtil.delegateTo;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IAtomicLongMocks {

    /**
     * Creates mocks IAtomicLong which wraps an AtomicLong
     **/
    public static IAtomicLong mockIAtomicLong() {
        final AtomicLong atomicLong = new AtomicLong(); // keeps actual value
        IAtomicLong iAtomicLong = mock(IAtomicLong.class);

        when(iAtomicLong.getAndIncrement()).then(delegateTo(atomicLong));
        when(iAtomicLong.compareAndSet(anyLong(), anyLong())).then(delegateTo(atomicLong));

        return iAtomicLong;
    }
}
