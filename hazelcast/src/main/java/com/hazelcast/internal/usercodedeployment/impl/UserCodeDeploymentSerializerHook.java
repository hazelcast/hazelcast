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

package com.hazelcast.internal.usercodedeployment.impl;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.internal.usercodedeployment.impl.operation.ClassDataFinderOperation;
import com.hazelcast.internal.usercodedeployment.impl.operation.DeployClassesOperation;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.internal.util.ConstructorFunction;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.USER_CODE_DEPLOYMENT_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.USER_CODE_DEPLOYMENT_DS_FACTORY_ID;

public class UserCodeDeploymentSerializerHook implements DataSerializerHook {
    public static final int F_ID = FactoryIdHelper.getFactoryId(USER_CODE_DEPLOYMENT_DS_FACTORY,
            USER_CODE_DEPLOYMENT_DS_FACTORY_ID);

    public static final int CLASS_DATA = 0;
    public static final int CLASS_DATA_FINDER_OP = 1;
    public static final int DEPLOY_CLASSES_OP = 2;

    public static final int LEN = DEPLOY_CLASSES_OP + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];
        constructors[CLASS_DATA] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ClassData();
            }
        };
        constructors[CLASS_DATA_FINDER_OP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ClassDataFinderOperation();
            }
        };
        constructors[DEPLOY_CLASSES_OP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new DeployClassesOperation();
            }
        };
        return new ArrayDataSerializableFactory(constructors);
    }
}
