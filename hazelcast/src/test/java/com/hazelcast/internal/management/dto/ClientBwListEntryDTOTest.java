package com.hazelcast.internal.management.dto;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientBwListEntryDTOTest {

    @Test
    public void testEqualsAndHashCode() {
        EqualsVerifier.forClass(ClientBwListEntryDTO.class)
                      .allFieldsShouldBeUsed()
                      .suppress(Warning.NULL_FIELDS, Warning.NONFINAL_FIELDS)
                      .verify();
    }

}
