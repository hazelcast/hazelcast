/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.query.physical;

import com.hazelcast.cluster.Member;

public class SendDestination {

    private final Member member;
    private final int stripe;
    private int thread;

    public SendDestination(Member member, int stripe) {
        this.member = member;
        this.stripe = stripe;
    }

    public Member getMember() {
        return member;
    }

    public int getStripe() {
        return stripe;
    }

    public int getThread() {
        return thread;
    }

    /**
     * Callback invoked when the thread for the given strip is resolved.
     *
     */
    private void onThreadResolved(int thread) {
        this.thread = thread;
    }
}
