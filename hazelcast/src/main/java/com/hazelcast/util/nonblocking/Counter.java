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

package com.hazelcast.util.nonblocking;

/*
 * Written by Cliff Click and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 *
 * Original work on http://high-scale-lib.sourceforge.net/
 */

/**
 * A simple high-performance counter.  Merely renames the extended {@link
 * ConcurrentAutoTable} class to be more obvious.
 * {@link ConcurrentAutoTable} already has a decent
 * counting API.
 *
 * @author Cliff Click
 * @since 1.5
 */
public class Counter extends ConcurrentAutoTable {

    // Add the given value to current counter value.  Concurrent updates will
    // not be lost, but addAndGet or getAndAdd are not implemented because but
    // the total counter value is not atomically updated.
    //public void add( long x );
    //public void decrement();
    //public void increment();

    // Current value of the counter.  Since other threads are updating furiously
    // the value is only approximate, but it includes all counts made by the
    // current thread.  Requires a pass over all the striped counters.
    //public long get();
    //public int  intValue();
    //public long longValue();

    // A cheaper 'get'.  Updated only once/millisecond, but fast as a simple
    // load instruction when not updating.
    //public long estimate_get( );

}