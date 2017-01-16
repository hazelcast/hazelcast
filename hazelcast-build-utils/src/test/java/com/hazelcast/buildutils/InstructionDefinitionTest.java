package com.hazelcast.buildutils;

import aQute.lib.osgi.Instruction;
import com.hazelcast.buildutils.HazelcastManifestTransformer.InstructionDefinition;
import com.hazelcast.buildutils.HazelcastManifestTransformer.PackageDefinition;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class InstructionDefinitionTest {

    private InstructionDefinition instructionDefinition;

    @Before
    public void setUp() {
        PackageDefinition definition = new PackageDefinition("packageName", true, "version", Collections.<String>emptySet());
        Instruction instruction = mock(Instruction.class);

        instructionDefinition = new InstructionDefinition(definition, instruction);
    }

    @Test
    public void testToString() {
        assertNotNull(instructionDefinition.toString());
        assertTrue(instructionDefinition.toString().contains("InstructionDefinition"));
    }
}
