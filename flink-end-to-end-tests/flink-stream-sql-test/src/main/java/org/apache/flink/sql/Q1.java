package org.apache.flink.sql;


import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;


public class Q1 {

//    ORDERKEY; //0
//    PARTKEY; //1
//    SUPPKEY; //2
//    LINENUMBER; //3
//    QUANTITY; //4
//    EXTENDEDPRICE; //5
//    DISCOUNT; //6
//    TAX; //7
//    RETURNFLAG; //8
//    LINESTATUS; //9
//    SHIPDATE; //10
//    COMMITDATE; //11
//    RECEIPTDATE; //12
//    SHIPINSTRUCT; //13
//    SHIPMODE; //14
//    COMMENT; //15

    public static class LineItemToTuple11Map implements MapFunction<Row, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long> map(Row lineItem) throws Exception {

            return Tuple11.of((String)lineItem.getField(8), (String)lineItem.getField(9),
                    ((Float)lineItem.getField(4)).longValue(),
                    ((Float)lineItem.getField(5)).doubleValue(),
                    ((Float)lineItem.getField(6)).doubleValue(),
                    (double) ((float) lineItem.getField(6) * (1 - (float) lineItem.getField(5))),
                    (double) ((float) lineItem.getField(6) * (1 - (float) lineItem.getField(5)) * (1
                            + (float) lineItem.getField(7))),
                    ((Float)lineItem.getField(4)).longValue(),
                    ((Float)lineItem.getField(5)).doubleValue(),
                    ((Float)lineItem.getField(6)).doubleValue(),
                    1L);
        }
    }

    public static class SumAndAvgLineItemReducer implements ReduceFunction<Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long> reduce(
                Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long> value1,
                Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long> value2) throws Exception {
            Long count = value1.f10 + value2.f10;
            Long sumQty = value1.f2 + value2.f2;
            Double sumBasePrice = value1.f3 + value2.f3;
            Double sumDisc = value1.f4 + value2.f4;
            Double sumDiscPrice = value1.f5 + value2.f5;
            Double sumCharge = value1.f6 + value2.f6;
            Long avgQty = sumQty / count;
            Double avgBasePrice = sumBasePrice / count;
            Double avgDisc = sumDisc / count;

            return Tuple11.of(
                    value1.f0,
                    value1.f1,
                    sumQty,
                    sumBasePrice,
                    sumDisc,
                    sumDiscPrice,
                    sumCharge,
                    avgQty,
                    avgBasePrice,
                    avgDisc,
                    count);
        }
    }

    public static class LineItemFlagAndStatusKeySelector implements KeySelector<Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>, Tuple2<String, String>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<String, String> getKey(Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long> lineItem) throws Exception {
            return Tuple2.of(lineItem.f0, lineItem.f1);
        }
    }

    public static void main(String[] args) throws Exception {

        String hdfsRoot = "hdfs://exp-cluster-m:8020/data/tpch/2G/";
        String tablePath = hdfsRoot+"lineitem.tbl";
        TypeInformation[] fieldTypes = new TypeInformation[]{
                BasicTypeInfo.INT_TYPE_INFO,BasicTypeInfo.INT_TYPE_INFO,BasicTypeInfo.INT_TYPE_INFO,BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.FLOAT_TYPE_INFO,BasicTypeInfo.FLOAT_TYPE_INFO,BasicTypeInfo.FLOAT_TYPE_INFO,BasicTypeInfo.FLOAT_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO};
        RowCsvInputFormat format = new RowCsvInputFormat(new Path(tablePath),fieldTypes,"\n","|");
        //ENV Definations
        final StreamExecutionEnvironment sEnv =
                StreamExecutionEnvironment.getExecutionEnvironment();

        sEnv.setParallelism(1);
//        sEnv.enableCheckpointing(4000);
//        sEnv.getConfig().setAutoWatermarkInterval(1000);
        DataStreamSource<Row> lineitem = sEnv.createInput(format);

        //Query
        lineitem.map(new LineItemToTuple11Map()).keyBy(new LineItemFlagAndStatusKeySelector()).reduce(new SumAndAvgLineItemReducer()).print();

        sEnv.execute("1-QUERY");


    }
}
