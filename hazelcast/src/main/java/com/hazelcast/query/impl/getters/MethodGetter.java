/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl.getters;

import java.lang.reflect.Method;

final class MethodGetter extends Getter {
    private final Method method;

    MethodGetter(Getter parent, Method method) {
        super(parent);
        this.method = method;
    }

    Object getValue(Object obj) throws Exception {
        Object paramObj = obj;
        paramObj = parent != null ? parent.getValue(paramObj) : paramObj;
        return paramObj != null ? method.invoke(paramObj) : null;
    }

    @Override
    Class getReturnType() {
        return this.method.getReturnType();
    }

    @Override
    boolean isCacheable() {
        return ReflectionHelper.THIS_CL.equals(method.getDeclaringClass().getClassLoader());
    }

    @Override
    public String toString() {
        return "MethodGetter [parent=" + parent + ", method=" + method.getName() + "]";
    }
}
