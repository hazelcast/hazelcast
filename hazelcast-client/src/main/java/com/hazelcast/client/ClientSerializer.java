///*
// * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package com.hazelcast.client;
//
//import com.hazelcast.nio.serialization.SerializerSupport;
//
//public final class ClientSerializer extends SerializerSupport {
//
////    public ClientSerializer(final ObjectSerializer objectSerializer) {
////        super(new ClientDataTypeSerializer(), new ObjectSerializerAdapter(objectSerializer));
////    }
////
////    private static final class ClientDataTypeSerializer extends DataSerializer {
////        @Override
////        protected Class loadClass(final String className) throws ClassNotFoundException {
////            String name = className;
////            if (className.equals("com.hazelcast.impl.CMap$Values")) {
////                name = "com.hazelcast.client.impl.Values";
////            } else if (className.equals("com.hazelcast.impl.base.KeyValue")) {
////                name = "com.hazelcast.client.impl.KeyValue";
////            }
////            return ClassLoaderUtil.loadClass(name);
////        }
////
////        @Override
////        protected String toClassName(final Object obj) throws ClassNotFoundException {
////            final String className = obj.getClass().getName();
////            if (!className.startsWith("com.hazelcast.client")) {
////                return className;
////            }
////            return "com.hazelcast" + className.substring("com.hazelcast.client".length());
////        }
////    }
//
//}
