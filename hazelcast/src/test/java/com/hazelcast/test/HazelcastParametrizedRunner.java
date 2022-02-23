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

package com.hazelcast.test;

import org.junit.runners.Parameterized;
import org.junit.runners.model.Statement;

/**
 * Custom parameterized runner which provides checks that resources have been
 * cleaned-up using {@link AfterClassesStatement}.
 *
 * Use as a replacement for {@link Parameterized}
 */
public class HazelcastParametrizedRunner extends Parameterized {

    public HazelcastParametrizedRunner(Class<?> klass) throws Throwable {
        super(klass);
    }

    @Override
    protected Statement withAfterClasses(Statement statement) {
        Statement originalStatement = super.withAfterClasses(statement);
        return new AfterClassesStatement(originalStatement);
    }
}
