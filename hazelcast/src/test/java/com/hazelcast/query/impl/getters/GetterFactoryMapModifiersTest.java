/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.query.impl.getters.AbstractMultiValueGetter.WRONG_MODIFIER_SUFFIX_ERROR;
import static com.hazelcast.query.impl.getters.GetterFactoryMapModifiersTest.Person.person;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.is;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class GetterFactoryMapModifiersTest {

    private static final String WRONG_INPUT_ANY = "[any";
    private static final String MODIFIER_SUFFIX = "[any]";
    private static final String MATCHING_MODIFIER = "['Joe']";
    private static final String MISMATCHING_MODIFIER = "['Jack']";

    private final Field employeesField;
    private final Method mapGetterMethod;

    private Department department;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        department = Department.of(
                person("Joe"),
                person("Jan")
        );
    }

    public GetterFactoryMapModifiersTest() throws NoSuchFieldException, NoSuchMethodException {
        employeesField = Department.class.getDeclaredField("employees");
        mapGetterMethod = Department.class.getDeclaredMethod("getEmployees");
    }

    @Test
    public void newFieldGetterReturnsCorrectTypeForModifierAny() throws Exception {
        // Act
        Getter fieldGetter = GetterFactory.newFieldGetter(department, null, employeesField, MODIFIER_SUFFIX);

        // Assert
        Class returnType = fieldGetter.getReturnType();
        Assert.assertThat(returnType, CoreMatchers.<Class>is(Person.class));
    }

    @Test
    public void newFieldGetterReturnCorrectTypeForModifierMatchingElement() throws Exception {
        // Act
        Getter fieldGetter = GetterFactory.newFieldGetter(department, null, employeesField, MATCHING_MODIFIER);

        // Assert
        Class returnType = fieldGetter.getReturnType();
        Assert.assertThat(returnType, CoreMatchers.<Class>is(Person.class));
    }

    @Test
    public void newFieldGetterReturnCorrectTypeForModifierMismatchingElement() throws Exception {
        // Act
        Getter fieldGetter = GetterFactory.newFieldGetter(department, null, employeesField, MISMATCHING_MODIFIER);

        // Assert
        Class returnType = fieldGetter.getReturnType();
        Assert.assertThat(returnType, CoreMatchers.<Class>is(Person.class));
    }

    @Test
    public void newFieldGetterIcorrectMapModifierThrowsIllegalArgumentExcetpion() throws Exception {
        // Arrange
        Department department = Department.of();
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(String.format(WRONG_MODIFIER_SUFFIX_ERROR, WRONG_INPUT_ANY));

        // Act
        Getter fieldGetter = GetterFactory.newFieldGetter(department, null, employeesField, WRONG_INPUT_ANY);

        // Assert
        Class returnType = fieldGetter.getReturnType();
        Assert.assertThat(returnType, CoreMatchers.<Class>is(Person.class));
    }

    @Test
    public void newFieldGetterCorrectMapModifierReturnsNullReturnTypeFroEmptyMap() throws Exception {
        // Arrange
        Department department = Department.of();

        // Act
        Getter fieldGetter = GetterFactory.newFieldGetter(department, null, employeesField, MODIFIER_SUFFIX);

        // Assert
        Class returnType = fieldGetter.getReturnType();
        Assert.assertThat(returnType, is(nullValue()));
    }

    @Test
    public void newMethodGetterReturnsCorrectTypeForModifierAny() throws Exception {
        // Act
        Getter fieldGetter = GetterFactory.newMethodGetter(department, null, mapGetterMethod, MODIFIER_SUFFIX);

        // Assert
        Class returnType = fieldGetter.getReturnType();
        Assert.assertThat(returnType, CoreMatchers.<Class>is(Person.class));
    }

    @Test
    public void newMethodGetterReturnCorrectTypeForModifierMatchingElement() throws Exception {
        // Act
        Getter fieldGetter = GetterFactory.newMethodGetter(department, null, mapGetterMethod, MATCHING_MODIFIER);

        // Assert
        Class returnType = fieldGetter.getReturnType();
        Assert.assertThat(returnType, CoreMatchers.<Class>is(Person.class));
    }

    @Test
    public void newMethodGetterReturnCorrectTypeForModifierMismatchingElement() throws Exception {
        // Act
        Getter fieldGetter = GetterFactory.newMethodGetter(department, null, mapGetterMethod, MISMATCHING_MODIFIER);

        // Assert
        Class returnType = fieldGetter.getReturnType();
        Assert.assertThat(returnType, CoreMatchers.<Class>is(Person.class));
    }


    @Test
    public void newMethodGetterIcorrectMapModifierThrowsIllegalArgumentExcetpion() throws Exception {
        // Arrange
        Department department = Department.of();
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(String.format(WRONG_MODIFIER_SUFFIX_ERROR, WRONG_INPUT_ANY));

        // Act
        Getter fieldGetter = GetterFactory.newMethodGetter(department, null, mapGetterMethod, WRONG_INPUT_ANY);

        // Assert
        Class returnType = fieldGetter.getReturnType();
        Assert.assertThat(returnType, CoreMatchers.<Class>is(Person.class));
    }

    @Test
    public void newMethodGetterCorrectMapModifierReturnsNullReturnTypeFroEmptyMap() throws Exception {
        // Arrange
        Department department = Department.of();

        // Act
        Getter fieldGetter = GetterFactory.newMethodGetter(department, null, mapGetterMethod, MODIFIER_SUFFIX);

        // Assert
        Class returnType = fieldGetter.getReturnType();
        Assert.assertThat(returnType, is(nullValue()));
    }


    static class Person {

        private final String name;

        private Person(String name) {
            this.name = name;
        }

        static Person person(String name) {
            return new Person(name);
        }
    }


    static class Department {

        Map<String, Person> employees = new HashMap<String, Person>();

        Map<String, Person> getEmployees() {
            return employees;
        }

        private static Department of(Person... employees) {
            Department department = new Department();
            for (Person employee : employees) {
                department.employees.put(employee.name, employee);
            }
            return department;
        }
    }
}