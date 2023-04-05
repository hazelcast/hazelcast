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

package com.hazelcast.instance.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.executejar.CommandLineJetProxy;
import com.hazelcast.instance.impl.executejar.MemberJetProxy;
import com.hazelcast.jet.JetService;

import javax.annotation.Nonnull;

public final class BootstrappedInstanceProxyFactory {

    private BootstrappedInstanceProxyFactory() {
    }

    public static BootstrappedInstanceProxy createWithCLIJetProxy(@Nonnull HazelcastInstance instance) {
        JetService jetService = instance.getJet();
        CommandLineJetProxy jetProxy = new CommandLineJetProxy(jetService);
        return new BootstrappedInstanceProxy(instance, jetProxy);
    }

    public static BootstrappedInstanceProxy createWithMemberJetProxy(@Nonnull HazelcastInstance instance) {
        JetService jetService = instance.getJet();
        MemberJetProxy jetProxy = new MemberJetProxy(jetService);
        return new BootstrappedInstanceProxy(instance, jetProxy);
    }
}
