package org.apache.flink.streaming.api.skew;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple1;

import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JoinSkewTest {
    @Test
    public void testJoin() throws Exception{
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<SkewedData> probeSource = env.readCsvFile("/Users/avinash/TexeraOrleans/dataset/skewed/81k-8k.csv")
                .fieldDelimiter(",")
                .includeFields("10000000000000").tupleType(SkewedData.class);

        DataSet<SkewedData> buildSource = env.readCsvFile("/Users/avinash/TexeraOrleans/dataset/small_input_9.csv")
                .fieldDelimiter(",")
                .includeFields("10000000000000").tupleType(SkewedData.class);

        DataSet<JoinedData> joinOutput =
                probeSource
                        .join(buildSource)
                        .where(0)
                        .equalTo(0)
                        .with(
                                new JoinFunction<SkewedData, SkewedData, JoinedData>() {
                                    @Override
                                    public JoinedData join(SkewedData p, SkewedData b) {
                                        return new JoinedData(p.region(), b.region());
                                    }
                                })
                        .groupBy(0)
                        .sum(2);

        joinOutput.print();

    }

    public static class SkewedData extends Tuple1<String> {
        public String region() {
            return this.f0;
        }
    }

    public static class JoinedData extends Tuple3<String, String, Integer> {

        public JoinedData() {}

        public JoinedData(
                String probeKey, String buildKey) {
            this.f0 = probeKey;
            this.f1 = buildKey;
            this.f2 = 1
        }

        public String getProbekey() {
            return this.f0;
        }

        public String getBuildKey() {
            return this.f1;
        }
    }
}
