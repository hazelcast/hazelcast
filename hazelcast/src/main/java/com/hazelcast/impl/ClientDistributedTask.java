/*
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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

import com.hazelcast.core.Member;

import java.io.*;
import java.util.Set;
import java.util.concurrent.Callable;


public class ClientDistributedTask<V> implements Serializable, Callable {
    private Callable<V> callable;

    private Member member;

    private Set<Member> members;

    private Object key;

    public ClientDistributedTask() {
    }

    public ClientDistributedTask(Callable<V> callable, Member member, Set<Member> members, Object key) {
        this.callable = callable;
        this.member = member;
        this.members = members;
        this.key = key;
    }

    public Object call() throws Exception {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Callable<V> getCallable() {
        return callable;
    }

    public Member getMember() {
        return member;
    }

    public Set<Member> getMembers() {
        return members;
    }

    public Object getKey() {
        return key;
    }
}
