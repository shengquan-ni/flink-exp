/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;

/**
 * Test program for the {@link StreamingFileSink} and {@link FileSink}.
 *
 * <p>Uses a source that steadily emits a deterministic set of records over 60 seconds, after which
 * it idles and waits for job cancellation. Every record has a unique index that is written to the
 * file.
 *
 * <p>The sink rolls on each checkpoint, with each part file containing a sequence of integers.
 * Adding all committed part files together, and numerically sorting the contents, should result in
 * a complete sequence from 0 (inclusive) to 60000 (exclusive).
 */
public enum GroupbySkewTest {
    ;

    public static void main(final String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.getConfig().setExecutionMode(ExecutionMode.BATCH);

        DataSet<SkewedData> probeSource =
                env.readCsvFile("/home/avinash/Documents/datasets/81k-8k.csv")
                        .fieldDelimiter(",")
                        .includeFields("10000000000000")
                        .tupleType(SkewedData.class)
                        .setParallelism(2);

        DataSet<MappedData> mapOutput =
                probeSource
                        .map(
                                new MapFunction<SkewedData, MappedData>() {
                                    @Override
                                    public MappedData map(SkewedData value) throws Exception {
                                        MappedData wc =
                                                new MappedData(value.region(), "Continent", 1);
                                        return wc;
                                    }
                                })
                        .setParallelism(3)
                        .groupBy(0)
                        .sum(2);

        //        DataSet<MappedData> reduceOutput = mapOutput.reduce(new
        // ReduceFunction<MappedData>() {
        //            private static final long serialVersionUID = 1L;
        //
        //            public MappedData reduce(MappedData value1, MappedData value2) {
        //                return new MappedData(
        //                        value1.f0, value1.f1, value1.f2+value2.f2);
        //            }
        //        });

        mapOutput.output(new DiscardingOutputFormat<MappedData>());
        System.out.println(env.getExecutionPlan());
        mapOutput.print();
    }

    /** Use first field for buckets. */
    public static class SkewedData extends Tuple1<String> {
        public String region() {
            return this.f0;
        }
    }

    /** Use first field for buckets. */
    public static class MappedData extends Tuple3<String, String, Integer> {

        public MappedData() {}

        public MappedData(String probeKey, String buildKey, int x) {
            this.f0 = probeKey;
            this.f1 = buildKey;
            this.f2 = x;
        }
    }

    /** Use first field for buckets. */
    public static class ReduceData extends Tuple2<String, Integer> {

        public ReduceData() {}

        public ReduceData(String probeKey, int x) {
            this.f0 = probeKey;
            this.f1 = x;
        }
    }
}
