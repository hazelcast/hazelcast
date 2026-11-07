/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.test.archunit;

import com.tngtech.archunit.core.domain.JavaAnnotation;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaMethod;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ConditionEvents;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedType;
import java.lang.reflect.Method;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

import static com.tngtech.archunit.lang.SimpleConditionEvent.violated;

/**
 * There are many different flavours of Java {@code null}ability annotation, and it's easily to be inconsistent, especially when
 * several are imported from transitive dependencies and often an IDE decides which one to import. Hence, we need to assert our
 * chosen flavour of annotation is the only one in use.
 */
public class UseConsistentNullabilityAnnotationCondition extends ArchCondition<JavaClass> {
    private static final Collection<String> ALLOWED_ANNOTATIONS = Set.of(
            // JSR305 is preferred
            "javax.annotation.Nonnull", "javax.annotation.Nullable",
            // `hazelcast-sql` uses Checkerframework for some reason
            "org.checkerframework.checker.nullness.qual.Nullable", "org.checkerframework.checker.nullness.qual.NonNull");
    private static final Collection<String> NULLABILITY_ANNOTATION_NAMES = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

    static {
        NULLABILITY_ANNOTATION_NAMES.add("NonNull");
        NULLABILITY_ANNOTATION_NAMES.add("NotNull");
        NULLABILITY_ANNOTATION_NAMES.add("Nullable");
    }

    public UseConsistentNullabilityAnnotationCondition() {
        super("Use only approved javax nullability annotations");
    }

    @Override
    public void check(JavaClass item, ConditionEvents events) {
        item.getAnnotations().forEach(annotation -> checkAnnotation(item, annotation, events));

        item.getFields()
                .forEach(field -> field.getAnnotations().forEach(annotation -> checkAnnotation(item, annotation, events)));

        item.getCodeUnits().forEach(codeUnit -> {
            codeUnit.getAnnotations().forEach(annotation -> checkAnnotation(item, annotation, events));

            codeUnit.getParameters().forEach(
                    parameter -> parameter.getAnnotations().forEach(annotation -> checkAnnotation(item, annotation, events)));
        });

        // Workaround https://github.com/TNG/ArchUnit/issues/1382
        item.getAllMethods().stream().map(JavaMethod::reflect).map(Method::getAnnotatedReturnType)
                .map(AnnotatedType::getAnnotations).flatMap(Arrays::stream).map(Annotation::annotationType)
                .forEach(annotationClass -> checkAnnotation(item, annotationClass.getSimpleName(), annotationClass.getName(),
                        events));
    }

    private static void checkAnnotation(JavaClass item, JavaAnnotation<?> annotation, ConditionEvents events) {
        JavaClass annotationType = annotation.getRawType();
        checkAnnotation(item, annotationType.getSimpleName(), annotationType.getName(), events);
    }

    private static void checkAnnotation(JavaClass item, String annotationSimpleName, String annotationName,
            ConditionEvents events) {
        if (NULLABILITY_ANNOTATION_NAMES.contains(annotationSimpleName) && !ALLOWED_ANNOTATIONS.contains(annotationName)) {
            events.add(
                    violated(item, MessageFormat.format("`{0}` uses nullability annotation `{1}` - use exclusively one of {2}.",
                            item.getName(), annotationName, ALLOWED_ANNOTATIONS)));
        }
    }

    public static ArchCondition<JavaClass> useConsistentNullabilityAnnotation() {
        return new UseConsistentNullabilityAnnotationCondition();
    }
}
