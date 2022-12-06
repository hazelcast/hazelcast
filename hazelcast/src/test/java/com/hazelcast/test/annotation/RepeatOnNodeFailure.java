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

package com.hazelcast.test.annotation;

import com.hazelcast.test.HazelcastTestSupport;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The tests that are specifically about the cluster topology, e.g. split-brain
 * tests, should be marked with this annotation, so that the test is repeated
 * upon a node failure. A test is repeated at most 5 times and 10 seconds are
 * waited before each repeat. The designated tests are not cancelled as soon as
 * a node fails, which will cause unnecessary waiting when {@code
 * assertXXXEventually()} methods are used. For early-exit, use {@link
 * HazelcastTestSupport#assertNoFailedInstances}.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface RepeatOnNodeFailure {

}
