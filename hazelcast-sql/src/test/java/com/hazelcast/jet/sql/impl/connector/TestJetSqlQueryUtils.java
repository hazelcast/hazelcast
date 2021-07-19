package com.hazelcast.jet.sql.impl.connector;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.sql.impl.JetPlan;
import com.hazelcast.sql.SqlExpectedResultType;
import com.hazelcast.sql.impl.SqlServiceImpl;

import javax.annotation.Nonnull;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

public class TestJetSqlQueryUtils {
    public static JetPlan planFromQuery(HazelcastInstance instance, String sql, @Nonnull List<Object> parameters) {
        SqlServiceImpl sqlService = (SqlServiceImpl) instance.getSql();
        Method prepareMethod;
        try {
            prepareMethod = sqlService.getClass()
                    .getDeclaredMethod("prepare", String.class, String.class, List.class, SqlExpectedResultType.class);
            prepareMethod.setAccessible(true);
            Object erasedPlan = prepareMethod.invoke(
                    sqlService,
                    null,
                    sql,
                    parameters,
                    SqlExpectedResultType.ANY
            );
            return (JetPlan) erasedPlan;
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
        return null;
    }
}
