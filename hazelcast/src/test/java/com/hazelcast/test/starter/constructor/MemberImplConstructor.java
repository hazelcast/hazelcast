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

package com.hazelcast.test.starter.constructor;

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.test.starter.HazelcastStarterConstructor;
import com.hazelcast.version.MemberVersion;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.test.starter.HazelcastProxyFactory.proxyArgumentsIfNeeded;
import static com.hazelcast.test.starter.ReflectionUtils.getFieldValueReflectively;

@HazelcastStarterConstructor(classNames = {"com.hazelcast.instance.MemberImpl"})
public class MemberImplConstructor extends AbstractStarterObjectConstructor {

    public MemberImplConstructor(Class<?> targetClass) {
        super(targetClass);
    }

    @Override
    Object createNew0(Object delegate) throws Exception {
        Object address = getFieldValueReflectively(delegate, "address");
        Object memberVersion = getMemberVersion(delegate);
        Boolean localMember = (Boolean) getFieldValueReflectively(delegate, "localMember");
        String uuid = (String) getFieldValueReflectively(delegate, "uuid");
        Object attributes = getFieldValueReflectively(delegate, "attributes");
        Boolean liteMember = (Boolean) getFieldValueReflectively(delegate, "liteMember");

        ClassLoader targetClassloader = targetClass.getClassLoader();
        Class<?> addressClass = targetClassloader.loadClass("com.hazelcast.nio.Address");
        Class<?> memberVersionClass = targetClassloader.loadClass("com.hazelcast.version.MemberVersion");

        try {
            // RU_COMPAT_3_11
            // obtain reference to constructor MemberImpl(Address address, MemberVersion version, boolean localMember,
            //                                            String uuid, Map<String, Object> attributes, boolean liteMember)
            Constructor<?> constructor = targetClass
                    .getDeclaredConstructor(addressClass, memberVersionClass, Boolean.TYPE, String.class, Map.class, Boolean.TYPE);
            Object[] args = new Object[]{address, memberVersion, localMember, uuid, attributes, liteMember};

            Object[] proxiedArgs = proxyArgumentsIfNeeded(args, targetClassloader);
            return constructor.newInstance(proxiedArgs);
        } catch (NoSuchMethodException e) {
            // Since 3.12, constructor args are
            // (Map<EndpointQualifier, Address> addresses, MemberVersion version, boolean localMember,
            // String uuid, Map<String, Object> attributes, boolean liteMember, int memberListJoinVersion,
            // HazelcastInstanceImpl instance)
            Class<?> hzImplClass = targetClassloader.loadClass("com.hazelcast.instance.HazelcastInstanceImpl");
            Constructor<?> constructor = targetClass
                    .getDeclaredConstructor(Map.class, memberVersionClass, Boolean.TYPE, String.class, Map.class, Boolean.TYPE,
                            Integer.TYPE, hzImplClass);
            constructor.setAccessible(true);
            Class<?> endpointQualifierClass =
                    targetClassloader.loadClass("com.hazelcast.instance.EndpointQualifier");
            Object memberEndpointQualifer = endpointQualifierClass.getDeclaredField("MEMBER").get(null);
            Map addressMap = new HashMap();
            addressMap.put(memberEndpointQualifer, address);
            Object[] args = new Object[] {addressMap, memberVersion, localMember, uuid, attributes, liteMember,
                                          MemberImpl.NA_MEMBER_LIST_JOIN_VERSION, null};
            return constructor.newInstance(proxyArgumentsIfNeeded(args, targetClassloader));
        }
    }

    private static Object getMemberVersion(Object delegate) throws Exception {
        // older Hazelcast versions don't have the version field
        try {
            return getFieldValueReflectively(delegate, "version");
        } catch (NoSuchFieldError e) {
            return MemberVersion.UNKNOWN;
        }
    }
}
