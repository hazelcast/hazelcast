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

package com.hazelcast.spring.boot.fortest;

import com.hazelcast.collection.ISet;
import com.hazelcast.map.IMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * Example annotation-based service.
 */
@Component
public class MySpringService {

    /** Test field-based annotation */
    @Autowired @Lazy
    public IMap<String, String> testMap;

    private final ISet<String> setForTests;

    /** Implicit constructor-based. Put -parameters javac flag in order to eliminate the need for {@link Qualifier}. */
    public MySpringService(@Lazy ISet<String> setForTests) {
        this.setForTests = setForTests;
    }

    public ISet<String> getISet() {
        return setForTests;
    }
}
