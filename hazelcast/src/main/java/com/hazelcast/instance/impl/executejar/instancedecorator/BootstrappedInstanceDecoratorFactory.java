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

package com.hazelcast.instance.impl.executejar.instancedecorator;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.executejar.jetservicedecorator.clientside.CommandLineJetServiceDecorator;
import com.hazelcast.instance.impl.executejar.jetservicedecorator.memberside.MemberJetServiceDecorator;
import com.hazelcast.jet.JetService;

import javax.annotation.Nonnull;

public final class BootstrappedInstanceDecoratorFactory {

    private BootstrappedInstanceDecoratorFactory() {
    }

    public static BootstrappedInstanceDecorator createWithCLIJetProxy(@Nonnull HazelcastInstance instance) {
        JetService jetService = instance.getJet();
        CommandLineJetServiceDecorator jetProxy = new CommandLineJetServiceDecorator(jetService);
        return new BootstrappedInstanceDecorator(instance, jetProxy);
    }

    public static BootstrappedInstanceDecorator createWithMemberJetProxy(@Nonnull HazelcastInstance instance) {
        JetService jetService = instance.getJet();
        MemberJetServiceDecorator jetProxy = new MemberJetServiceDecorator(jetService);
        return new BootstrappedInstanceDecorator(instance, jetProxy);
    }
}
