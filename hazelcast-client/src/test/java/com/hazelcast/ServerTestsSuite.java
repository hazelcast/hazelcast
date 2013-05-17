/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast;

import com.hazelcast.concurrent.atomiclong.AtomicLongTest;
import com.hazelcast.concurrent.countdownlatch.CountDownLatchTest;
import com.hazelcast.instance.StaticNodeFactory;
import com.hazelcast.util.ClientCompatibleTest;
import org.junit.runner.JUnitCore;
import org.junit.runner.Request;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class ServerTestsSuite {

    public static void main(String[] args) throws Exception {
        System.setProperty(StaticNodeFactory.HAZELCAST_TEST_USE_NETWORK, "true");
        System.setProperty(StaticNodeFactory.HAZELCAST_TEST_USE_CLIENT, "true");

        List<Class> classes = new ArrayList<Class>();
        classes.add(AtomicLongTest.class);
        classes.add(CountDownLatchTest.class);
        Class annotation = ClientCompatibleTest.class;
        JUnitCore core = new JUnitCore();
        core.addListener(new RunListener() {
            public void testFailure(Failure failure) throws Exception {
                System.err.println(failure.getTrace());
            }
        });
        List<String> failed = new ArrayList<String>();
        List<String> success = new ArrayList<String>();
        for (Class testClass : classes) {
            Method[] methods = testClass.getMethods();
            for (Method method : methods) {
                if (method.isAnnotationPresent(annotation)) {
                    if (method.isAnnotationPresent(org.junit.Test.class)) {
                        Request request = Request.method(testClass, method.getName());
                        Result result = core.run(request);
                        if (!result.wasSuccessful())
                            failed.add(testClass + "." + method.getName());
                        else
                            success.add(testClass + "." + method.getName());
                    }
                }
            }
        }
        if (success.size() > 0)
            System.out.println("The following Client tests have passed:");
        for (String test : success) {
            System.out.println(test);
        }
        if (failed.size() > 0)
            System.err.println("FAILURE!! SOME TESTS FAILED! # of failing Tests: " + failed.size());
        else {
            System.out.println("SUCCESS!! All TESTS PASSED! # of passed Tests: " + success.size());
        }
        for (String test : failed) {
            System.err.println(test);
        }
    }
}
