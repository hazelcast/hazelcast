package com.hazelcast.sql.impl.calcite.opt.distribution;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DistributionTraitTest {

    @Test
    public void testIsFullResultSetOnAllParticipants() {
        DistributionTraitDef traitDef = new DistributionTraitDef(2);
        DistributionTrait trait = new DistributionTrait(traitDef, DistributionType.ROOT);
        assertTrue(trait.isFullResultSetOnAllParticipants());

        trait = new DistributionTrait(traitDef, DistributionType.REPLICATED);
        assertTrue(trait.isFullResultSetOnAllParticipants());

        trait = new DistributionTrait(traitDef, DistributionType.PARTITIONED);
        assertFalse(trait.isFullResultSetOnAllParticipants());

        trait = new DistributionTrait(traitDef, DistributionType.ANY);
        assertFalse(trait.isFullResultSetOnAllParticipants());
    }

    @Test
    public void testIsFullResultSetOnAllParticipantsOneMember() {
        DistributionTraitDef traitDef = new DistributionTraitDef(1);

        for (DistributionType type : DistributionType.values()) {
            DistributionTrait trait = new DistributionTrait(traitDef, DistributionType.ROOT);
            assertTrue(trait.isFullResultSetOnAllParticipants());
        }
    }

}
