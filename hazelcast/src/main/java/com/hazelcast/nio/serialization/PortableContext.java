/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.serialization;

import com.hazelcast.core.ManagedContext;

import java.io.IOException;

public interface PortableContext {

    int getVersion();

    int getClassVersion(int factoryId, int classId);

    void setClassVersion(int factoryId, int classId, int version);

    ClassDefinition lookup(int factoryId, int classId, int version);

    ClassDefinition createClassDefinition(int factoryId, byte[] binary) throws IOException;

    ClassDefinition registerClassDefinition(ClassDefinition cd);

    ClassDefinition lookupOrRegisterClassDefinition(Portable portable) throws IOException;

    FieldDefinition getFieldDefinition(ClassDefinition cd, String name);

    ManagedContext getManagedContext();

}
