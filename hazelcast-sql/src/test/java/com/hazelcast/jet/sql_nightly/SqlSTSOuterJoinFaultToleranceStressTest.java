package com.hazelcast.jet.sql_nightly;

public class SqlSTSOuterJoinFaultToleranceStressTest extends SqlSTSInnerEquiJoinFaultToleranceStressTest {
    @Override
    protected String setupQuery() {
        return "CREATE JOB job OPTIONS ('processingGuarantee'='" + processingGuarantee + "') AS " +
                " SINK INTO " + resultMapName +
                " SELECT s1.__key, s2.this FROM s1 LEFT JOIN s2 ON s1.__key = s2.__key";
    }
}
