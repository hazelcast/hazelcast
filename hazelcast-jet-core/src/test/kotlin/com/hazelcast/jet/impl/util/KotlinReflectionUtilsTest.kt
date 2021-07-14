package com.hazelcast.jet.impl.util

import org.hamcrest.Matchers.containsInAnyOrder
import org.hamcrest.Matchers.hasSize
import org.junit.Assert.assertThat
import org.junit.Test

class KotlinReflectionUtilsTest {

    @Test
    fun shouldAddNestedAndAnonymousClasses() {
        // When
        val classes = ReflectionUtils.nestedClassesOf(OuterClass::class.java)

        // Then
        assertThat(classes, hasSize(4))
        assertThat(classes, containsInAnyOrder(
                OuterClass::class.java,
                OuterClass.NestedClass::class.java,
                Class.forName("com.hazelcast.jet.impl.util.KotlinReflectionUtilsTest\$OuterClass\$method\$1"),
                Class.forName("com.hazelcast.jet.impl.util.KotlinReflectionUtilsTest\$OuterClass\$method\$lambda\$1")
        ))
    }

    @Suppress("unused", "UNUSED_VARIABLE")
    class OuterClass {
        private fun method() {
            object : Any() {}

            val lambda = { }
        }

        class NestedClass
    }
}
