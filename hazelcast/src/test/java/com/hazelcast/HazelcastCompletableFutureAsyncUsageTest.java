package com.hazelcast;

import com.hazelcast.test.archunit.ArchUnitRules;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import org.junit.Test;

import static com.hazelcast.test.archunit.ModuleImportOptions.onlyCurrentModule;

public class HazelcastCompletableFutureAsyncUsageTest {

    @Test
    public void noClassUsesCompletableFuture() {
        String basePackage = "com.hazelcast";
        JavaClasses classes = new ClassFileImporter()
                .withImportOption(onlyCurrentModule())
                .importPackages(basePackage);

        ArchUnitRules.COMPLETABLE_FUTURE_ASYNC_USED_ONLY_WITH_EXPLICIT_EXECUTOR.check(classes);
    }
}
