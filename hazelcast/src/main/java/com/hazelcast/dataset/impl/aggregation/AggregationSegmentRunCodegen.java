package com.hazelcast.dataset.impl.aggregation;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.aggregation.impl.CountAggregator;
import com.hazelcast.aggregation.impl.DoubleSumAggregator;
import com.hazelcast.aggregation.impl.LongAverageAggregator;
import com.hazelcast.aggregation.impl.LongSumAggregator;
import com.hazelcast.aggregation.impl.MaxAggregator;
import com.hazelcast.aggregation.impl.MinAggregator;
import com.hazelcast.dataset.AggregationRecipe;
import com.hazelcast.dataset.impl.RecordModel;
import com.hazelcast.dataset.impl.SegmentRunCodegen;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Set;

public class AggregationSegmentRunCodegen extends SegmentRunCodegen {

    private final Set<Field> extractedFields;
    private final Class<?> projectionClass;
    private final Aggregator aggregator;

    public AggregationSegmentRunCodegen(String compilationId, AggregationRecipe aggregationRecipe, RecordModel recordModel) {
        super(compilationId, aggregationRecipe.getPredicate(), recordModel);
        try {
            this.projectionClass = getClass().getClassLoader().loadClass(aggregationRecipe.getProjectionClassName());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        try {
            Class<Aggregator> aggregatorClass = (Class<Aggregator>) getClass().getClassLoader().loadClass(aggregationRecipe.getAggregatorClassName());
            this.aggregator = aggregatorClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        this.extractedFields = extractedFields();
    }

    @Override
    public String className() {
        return "Aggregation_" + compilationId;
    }

    public Field field() {
        return extractedFields.iterator().next();
    }

    @Override
    public void generate() {
        add("import java.util.*;\n");
        add("public class " + className() + " extends com.hazelcast.dataset.impl.aggregation.AggregationSegmentRun {\n\n");
        addParamFields();
        addAggregatorField();
        addBindMethod();
        addResultMethod();
        addRunFullScanMethod();
        addRunWithIndexMethod();
        add("}\n");
    }

    private void addAggregatorField() {
        add("    private final " + aggregator.getClass().getName() + " aggregator = new " + aggregator.getClass().getName() + "();\n\n");
    }

    private void addResultMethod() {
        add("    public " + Aggregator.class.getName() + " result(){return aggregator;}\n\n");
    }

    private void addRunWithIndexMethod(){
        add("    protected void runWithIndex(){\n");
        add("    }\n\n");
    }

    private void addRunFullScanMethod() {
        add("    protected void runFullScan(){\n");

        int unrollCount = 4;

        for (int unrollIndex = 0; unrollIndex < unrollCount; unrollIndex++) {
            if (aggregator instanceof CountAggregator) {
                add("       long result_%d=0;\n", unrollIndex);
            } else if (aggregator instanceof LongSumAggregator) {
                add("       long result_%d=0;\n", unrollIndex);
            } else if (aggregator instanceof MinAggregator) {
                add("       long result_%d=Long.MAX_VALUE;\n", unrollIndex);
            } else if (aggregator instanceof MaxAggregator) {
                add("       long result_%d=Long.MIN_VALUE;\n", unrollIndex);
            } else if (aggregator instanceof LongAverageAggregator) {
                add("       long sum_%d=0;\n", unrollIndex);
                add("       long count_%d=0;\n", unrollIndex);
            } else if (aggregator instanceof DoubleSumAggregator) {
                add("       double result_%d=0;\n", unrollIndex);
            } else {
                throw new RuntimeException();
            }
        }

        add("       long recordAddress=dataAddress;\n");
        add("       for(int l=0;l<recordCount;l+=%d){\n", unrollCount);

        for (int unrollIndex = 0; unrollIndex < unrollCount; unrollIndex++) {
            add("           if(");
            toCode(query, unrollIndex);
            add("){\n");

            if (aggregator instanceof CountAggregator) {
                add("               result_%d+=1;\n", unrollIndex);
            } else if (aggregator instanceof LongSumAggregator) {
                add("               result_%d+=", unrollIndex);
                addGetField(field().getName(), unrollIndex);
                add(";\n");
            } else if (aggregator instanceof LongAverageAggregator) {
                add("               count_%d+=1;\n", unrollIndex);
                add("               sum_%d+=", unrollIndex);
                addGetField(field().getName(), unrollIndex);
                add(";\n");
            } else if (aggregator instanceof MinAggregator) {
                add("               long value_%d=", unrollIndex);
                addGetField(field().getName(), unrollIndex);
                add(";\n");
                add("               if(value_%d<result_%d) result_%d=value_%d;\n",
                        unrollIndex, unrollIndex, unrollIndex, unrollIndex);
            } else if (aggregator instanceof MaxAggregator) {
                add("               long value_%d=", unrollIndex);
                addGetField(field().getName(), unrollIndex);
                add(";\n");
                add("               if(value_%d>result_%d) result_%d=value_%d;\n",
                        unrollIndex, unrollIndex, unrollIndex, unrollIndex);
            } else if (aggregator instanceof DoubleSumAggregator) {
                add("               result_%d+=", unrollIndex);
                addGetField(field().getName(), unrollIndex);
                add(";\n");
            }
            //append("                count++;\n");
            add("           }\n");
        }

        add("           recordAddress+=%d*recordDataSize;\n", unrollCount);
        add("        }\n");
        if (aggregator instanceof LongAverageAggregator) {
            add("        long sum=0;\n");
            add("        long count=0;\n");

            for (int unrollIndex = 0; unrollIndex < unrollCount; unrollIndex++) {
                add("        sum+=sum_%d;\n", unrollIndex);
                add("        count+=count_%d;\n", unrollIndex);
            }

            add("        aggregator.init(sum,count);\n");
        } else {
            for (int unrollIndex = 0; unrollIndex < unrollCount; unrollIndex++) {
                add("        aggregator.accumulate(result_%d);\n", unrollIndex);
            }
        }
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
