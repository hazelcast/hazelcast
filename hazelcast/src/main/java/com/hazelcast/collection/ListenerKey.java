///*
// * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
//package com.hazelcast.collection;
//
//import com.hazelcast.nio.serialization.Data;
//
//import java.util.EventListener;
//
///**
// * @ali 1/9/13
// */
//public class ListenerKey {
//
//    private final String name;
//    private final Data key;
//    private final EventListener listener;
//
//    public ListenerKey(String name, Data key, EventListener listener) {
//        this.name = name;
//        this.key = key;
//        this.listener = listener;
//    }
//
//    @Override
//    public boolean equals(Object o) {
//        if (this == o) return true;
//        if (!(o instanceof ListenerKey)) return false;
//
//        ListenerKey that = (ListenerKey) o;
//
//        if (key != null ? !key.equals(that.key) : that.key != null) return false;
//        if (!listener.equals(that.listener)) return false;
//        if (!name.equals(that.name)) return false;
//
//        return true;
//    }
//
//    @Override
//    public int hashCode() {
//        int result = name.hashCode();
//        result = 31 * result + (key != null ? key.hashCode() : 0);
//        result = 31 * result + listener.hashCode();
//        return result;
//    }
//}
