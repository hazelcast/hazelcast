package org.apache.calcite.plan.hep;

import org.apache.calcite.plan.RelOptRule;

import java.util.ArrayList;
import java.util.List;

public class HazelcastHepProgramBuilder {

    private final List<HepInstruction> instructions = new ArrayList<>();

    public HazelcastHepProgramBuilder addRuleInstance(RelOptRule rule) {
        return addInstruction(new HepInstruction.RuleInstance(rule));
    }

    private HazelcastHepProgramBuilder addInstruction(HepInstruction instruction) {
        instructions.add(instruction);
        return this;
    }

    public HepProgram build() {
        return new HepProgram(instructions);
    }
}
