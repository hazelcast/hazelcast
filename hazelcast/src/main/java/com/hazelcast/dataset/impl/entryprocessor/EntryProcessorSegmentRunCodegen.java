package com.hazelcast.dataset.impl.entryprocessor;

import com.hazelcast.dataset.AddMutator;
import com.hazelcast.dataset.EntryProcessorRecipe;
import com.hazelcast.dataset.FieldMutator;
import com.hazelcast.dataset.MultiplyMutator;
import com.hazelcast.dataset.Mutator;
import com.hazelcast.dataset.RecordMutator;
import com.hazelcast.dataset.impl.RecordModel;
import com.hazelcast.dataset.impl.SegmentRunCodegen;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Set;

public class EntryProcessorSegmentRunCodegen extends SegmentRunCodegen {

    private final Mutator mutator;
    private final Class projectionClass;
    public Field field;

    public EntryProcessorSegmentRunCodegen(String preparationId, EntryProcessorRecipe recipe, RecordModel recordModel) {
        super(preparationId, recipe.getPredicate(), recordModel);

        this.mutator = recipe.getMutator();
        this.projectionClass = recordModel.getRecordClass();
        if (mutator instanceof FieldMutator) {
            FieldMutator fieldMutator = ((FieldMutator) mutator);
            field = recordModel.getField(fieldMutator.getField());
        }
    }

    @Override
    public String className() {
        return "EntryProcessorSegmentRun_" + compilationId;
    }

    @Override
    public void generate() {
        add("import java.util.*;\n");
        add("public class " + className() + " extends com.hazelcast.dataset.impl.entryprocessor.EntryProcessorSegmentRun {\n\n");

        addMutatorField();
        add("\n");

        addRunFullScanMethod();
        addRunWithIndexMethod();
        addParamFields();
        addBindMethod();
        add("}\n");
    }

    private void addMutatorField() {
        add("    private final " + mutator.getClass().getName() + " mutator = new " + mutator.getClass().getName() + "();\n");
    }

    private void addRunWithIndexMethod() {
        add("    protected void runWithIndex(){\n");
        add("    }\n\n");
    }

    private void addRunFullScanMethod() {
        add("    protected void runFullScan(){\n");
        add("       long recordAddress=dataAddress;\n");
        add("       " + projectionClass.getName() + " object=new " + projectionClass.getName() + "();\n");
        add("       for(int l=0;l<recordCount;l++){\n");
        add("           if(");
        toCode(query, 0);
        add("){\n");

        if (mutator instanceof RecordMutator) {
            for (Field field : extractedFields()) {
                add("               object.").add(field.getName()).add("=");
                addGetField(field.getName(), 0);
                add(";\n");
            }
            add("               if(mutator.mutate(object)){\n");
            for (Field field : extractedFields()) {
                add("                   ");
                addPutField(field.getName(), 0, "object." + field.getName());
                add(";\n");
            }
            add("               }\n");
        } else {
            add("               " + field.getType().getName() + " value=");

            if (mutator instanceof AddMutator) {
                addGetField(field.getName(), 0);
                //todo
                add("+1");
            } else if (mutator instanceof MultiplyMutator) {
                addGetField(field.getName(), 0);
                // todo
                add("*2");
            } else {
                //todo
                throw new RuntimeException();
            }

            add(";\n");
            add("               ");
            addPutField(field.getName(), 0, "value");
            add(";\n");
        }
        add("           }\n");
        add("           recordAddress+=%s;\n", recordModel.getSize());
        add("        }\n");
        add("    }\n\n");
    }

    private Set<Field> extractedFields() {
        Set<Field> fields = new HashSet<Field>();

        for (Field f : projectionClass.getDeclaredFields()) {
            if (Modifier.isStatic(f.getModifiers())) {
                continue;
            }

            Field recordField = recordModel.getField(f.getName());
            if (recordField == null) {
                throw new RuntimeException(
                        "Field '" + projectionClass.getName() + '.' + f.getName()
                                + "' is not found on value-class '" + recordModel.getRecordClass() + "'");
            }

            if (!recordField.getType().equals(f.getType())) {
                throw new RuntimeException(
                        "Field '" + projectionClass.getName() + '.' + f.getName()
                                + "' has a different type compared to '" + recordModel.getRecordClass() + "'");
            }

            fields.add(f);
        }

        return fields;
    }
}
