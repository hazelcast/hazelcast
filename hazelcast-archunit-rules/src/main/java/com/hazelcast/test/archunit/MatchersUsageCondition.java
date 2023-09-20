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
package com.hazelcast.test.archunit;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaMethodCall;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ConditionEvents;

import static com.tngtech.archunit.lang.SimpleConditionEvent.violated;

/**
 * Hamcrest is great, but AssertJ is even better.
 * Also, Hamcrest matchers tend to cause the usage of {@code org.junit.Assert#assertThat}
 * (didn't add link to not depend on deprecated method), which is deprecated - and AssertJ is better for fluent assertions.
 */
public class MatchersUsageCondition extends ArchCondition<JavaClass> {

    public MatchersUsageCondition() {
        super("Use AssertJ instead of Hamcrest matchers");
    }

    @Override
    public void check(JavaClass item, ConditionEvents events) {
        for (JavaMethodCall methodCall : item.getMethodCallsFromSelf()) {
            if (methodCall.getTarget().getFullName().contains("org.hamcrest")) {
                String violatingMethodName = methodCall.getOwner().getFullName();
                events.add(violated(item, violatingMethodName + ":" + methodCall.getLineNumber()
                        + " calls Hamcrest Matcher " + methodCall.getName() + ". You should consider AssertJ matchers."));
            }
        }
    }

    public static ArchCondition<JavaClass> notUseHamcrestMatchers() {
        return new MatchersUsageCondition();
    }
}
