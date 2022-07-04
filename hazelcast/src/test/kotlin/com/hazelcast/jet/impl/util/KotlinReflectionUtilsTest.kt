package com.hazelcast.jet.impl.util

import com.hazelcast.test.HazelcastParallelClassRunner
import com.hazelcast.test.annotation.ParallelJVMTest
import com.hazelcast.test.annotation.QuickTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.junit.experimental.categories.Category
import org.junit.runner.RunWith

@RunWith(HazelcastParallelClassRunner::class)
@Category(QuickTest::class, ParallelJVMTest::class)
class KotlinReflectionUtilsTest {

    @Test
    fun shouldAddNestedAndAnonymousClasses() {
        // When
        val classes = ReflectionUtils.nestedClassesOf(OuterClass::class.java)

        // Then
        assertThat(classes).hasSize(4)
        assertThat(classes).containsExactlyInAnyOrder(
                OuterClass::class.java,
                OuterClass.NestedClass::class.java,
                Class.forName("com.hazelcast.jet.impl.util.KotlinReflectionUtilsTest\$OuterClass\$method\$1"),
                Class.forName("com.hazelcast.jet.impl.util.KotlinReflectionUtilsTest\$OuterClass\$method\$lambda\$1")
        )
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
