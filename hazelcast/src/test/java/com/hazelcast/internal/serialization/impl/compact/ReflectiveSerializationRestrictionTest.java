/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.internal.serialization.impl.compact;

import com.hazelcast.config.ClassFilter;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.impl.AbstractSerializationService;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.List;
import java.util.stream.Stream;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(QuickTest.class)
public class ReflectiveSerializationRestrictionTest
        extends AbstractReflectiveSerializationRestrictionTest {

    @Parameters(name = "underTest={0}")
    public static List<Object[]> parameters() {
        return Stream.of(new RestrictionTestDto(5),
                new RestrictionTestDto[] { new RestrictionTestDto(4), new RestrictionTestDto(3)}
        ).map(e -> new Object[]{e}).toList();
    }

    public static class RestrictionTestDto  {
        private int n;

        public RestrictionTestDto(int n) {
            this.n = n;
        }

        public int getN() {
            return n;
        }

        @Override
        public String toString() {
            return "RestrictionTestDto{n=" + n + '}';
        }
    }

    @Override
    protected AbstractSerializationService getSerializationServiceToTest(HazelcastInstance hz) {
        ClassFilter blockList = new ClassFilter();
        blockList.addClasses(RestrictionTestDto.class.getName());
        return overrideBlockList(hz, blockList);
    }

    @Override
    protected boolean isSuccessExpected() {
        return false;
    }
}
