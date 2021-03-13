import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public enum GroupbySkewTest {
    ;
    public static void main(final String[] args) throws Exception{
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.getConfig().setExecutionMode(ExecutionMode.BATCH);

        DataSet<SkewedData> probeSource = env.readCsvFile("/home/avinash/Documents/datasets/81k-8k.csv")
                .fieldDelimiter(",")
                .includeFields("10000000000000").tupleType(SkewedData.class);

        DataSet<MappedData> mapOutput = probeSource.map(new MapFunction<SkewedData, MappedData>() {
            @Override
            public MappedData map(SkewedData value) throws Exception {
                MappedData wc = new MappedData(value.region(), "Continent", 1);
                return wc;

            }
        })
                .groupBy(0).sum(2) ;

//        DataSet<MappedData> reduceOutput = mapOutput.reduce(new ReduceFunction<MappedData>() {
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

    public static class SkewedData extends Tuple1<String> {
        public String region() {
            return this.f0;
        }
    }

    public static class MappedData extends Tuple3<String, String, Integer> {

        public MappedData() {}

        public MappedData(
                String probeKey, String buildKey, int x) {
            this.f0 = probeKey;
            this.f1 = buildKey;
            this.f2 = x;
        }
    }

    public static class ReduceData extends Tuple2<String, Integer> {

        public ReduceData() {}

        public ReduceData(
                String probeKey, int x) {
            this.f0 = probeKey;
            this.f1 = x;
        }
    }
}
