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

package com.hazelcast.client.usercodedeployment;

import com.hazelcast.client.impl.spi.impl.ClientUserCodeDeploymentService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientUserCodeDeploymentUtilTest {

    public interface A {

    }

    public interface X {

    }

    public interface B extends X {

    }

    public static class C implements A, Runnable {
        @Override
        public void run() {

        }
    }

    public static class D implements B {

    }

    public static class E extends D {

    }

    public static class F extends C {

    }

    public static class G implements A, B {

    }

    public static boolean isSortedByInheritance(List<Class> classList) {
        for (int i = 0; i < classList.size(); i++) {
            for (int j = i + 1; j < classList.size(); j++) {
                Class aClass = classList.get(i);
                Class bClass = classList.get(j);
                if (bClass.isAssignableFrom(aClass)) {
                    return false;
                }
            }
        }
        return true;
    }

    @Test
    public void testSortByInheritance() {
        List<Class<?>> objects = Arrays.asList(X.class, F.class, E.class, B.class, A.class, D.class, C.class, G.class);
        Collections.shuffle(objects);
        List<Class> classes = ClientUserCodeDeploymentService.sortByInheritance(objects);
        assertTrue("Unable to sort following list " + objects, isSortedByInheritance(classes));
    }
}
