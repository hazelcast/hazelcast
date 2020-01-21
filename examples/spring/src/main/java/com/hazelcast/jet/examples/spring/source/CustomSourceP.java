/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.examples.spring.source;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.examples.spring.dao.UserDao;
import com.hazelcast.jet.examples.spring.model.User;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.spring.context.SpringAware;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.preferLocalParallelismOne;

/**
 * A processor which uses an auto-wired DAO to find all users and emit them to downstream.
 * {@code @SpringAware} annotation enables this auto-wiring functionality.
 */
@SpringAware
public class CustomSourceP extends AbstractProcessor {

    private Traverser<User> traverser;

    @Autowired
    private transient UserDao userDao;

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    protected void init(@Nonnull Context context) {
        traverser = Traversers.traverseIterable(userDao.findAll());
    }

    @Override
    public boolean complete() {
        return emitFromTraverser(traverser);
    }

    public static BatchSource<User> customSource() {
        return Sources.batchFromProcessor("custom-source", preferLocalParallelismOne(CustomSourceP::new));
    }
}
