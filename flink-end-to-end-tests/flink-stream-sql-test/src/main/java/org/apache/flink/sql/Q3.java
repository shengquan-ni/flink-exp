package org.apache.flink.sql;


import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


public class Q3 {

public static class OrderDateFilter implements FilterFunction<Row> {
    private static final long serialVersionUID = 1L;

    private final DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    private final Date date;

    public OrderDateFilter() throws ParseException {
        date = sdf.parse("1995-03-12");
    }

    public OrderDateFilter(String yyyymmdd) throws ParseException {
        date = sdf.parse(yyyymmdd);
    }

    // int year = OrdersSource.getYear(o.getOrderDate());
    @Override
    public boolean filter(Row o) throws ParseException {
        String dateValue = o.getFieldAs(4);
        return sdf.parse(dateValue).before(date);
    }
}

    public static class OrderKeyedByCustomerProcessFunction extends KeyedProcessFunction<Long, Row, ShippingPriorityItem> {
        private static final long serialVersionUID = 1L;

        @Override
        public void processElement(Row order, KeyedProcessFunction<Long, Row, ShippingPriorityItem>.Context context,
                                   Collector<ShippingPriorityItem> out) {
            try {
                    // System.out.println("Customer: " + customer + " - Order: " + order);
                        try {
                            ShippingPriorityItem spi = new ShippingPriorityItem(((Integer)order.getFieldAs(0)).longValue(), 0.0,
                                    order.getFieldAs(4), order.getFieldAs(5));
                            out.collect(spi);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class SumShippingPriorityItem extends RichReduceFunction<ShippingPriorityItem> {
        private static final long serialVersionUID = 1L;


        @Override
        public ShippingPriorityItem reduce(ShippingPriorityItem value1, ShippingPriorityItem value2) {
            ShippingPriorityItem shippingPriorityItem = null;
            try {

                shippingPriorityItem = new ShippingPriorityItem(value1.getOrderkey(),
                        value1.getRevenue() + value2.getRevenue(), value1.getOrderdate(), value1.getShippriority());
            } catch (Exception e) {
                e.printStackTrace();
            }
            return shippingPriorityItem;
        }
    }


    public static void main(String[] args) throws Exception {

        //ENV Definations
        final StreamExecutionEnvironment sEnv =
                StreamExecutionEnvironment.getExecutionEnvironment();

        sEnv.setParallelism(1);
//        sEnv.enableCheckpointing(4000);
//        sEnv.getConfig().setAutoWatermarkInterval(1000);

        String hdfsRoot = "hdfs://exp-cluster-m:8020/data/tpch/2G/";
        String lineitemPath = hdfsRoot+"lineitem.tbl";
        TypeInformation[] lineitemFields = new TypeInformation[]{
                BasicTypeInfo.INT_TYPE_INFO,BasicTypeInfo.INT_TYPE_INFO,BasicTypeInfo.INT_TYPE_INFO,BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.FLOAT_TYPE_INFO,BasicTypeInfo.FLOAT_TYPE_INFO,BasicTypeInfo.FLOAT_TYPE_INFO,BasicTypeInfo.FLOAT_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO};
        RowCsvInputFormat lineitemFormat = new RowCsvInputFormat(new Path(lineitemPath),lineitemFields,"\n","|");
        DataStream<Row> lineitem = sEnv.createInput(lineitemFormat);
        String orderPath = hdfsRoot+"orders.tbl";
        TypeInformation[] orderFields = new TypeInformation[]{
                BasicTypeInfo.INT_TYPE_INFO,BasicTypeInfo.INT_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.FLOAT_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO};
        RowCsvInputFormat orderFormat = new RowCsvInputFormat(new Path(orderPath),orderFields,"\n","|");
        DataStream<Row> order = sEnv.createInput(orderFormat);
//        String customerPath = "customer.tbl";
//        TypeInformation[] customerFields = new TypeInformation[]{
//                BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.FLOAT_TYPE_INFO,
//                BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO};
//        RowCsvInputFormat customerFormat = new RowCsvInputFormat(new Path(customerPath),customerFields,"\n","|");
//        DataStreamSource<Row> customer = sEnv.createInput(customerFormat);

        // Filter all Orders with o_orderdate < 12.03.1995
        DataStream<Row> ordersFiltered = order
                .filter(new OrderDateFilter("1995-03-12"));
        // Join customers with orders and package them into a ShippingPriorityItem
        DataStream<ShippingPriorityItem> customerWithOrders = ordersFiltered
                .keyBy(x -> ((Integer)x.getFieldAs(0)).longValue())
                .process(new OrderKeyedByCustomerProcessFunction());

        DataStream<ShippingPriorityItem> result = customerWithOrders
                .keyBy(x -> x.getOrderkey()).join(lineitem).where(x ->x.getOrderkey()).equalTo(x ->((Integer)x.getField(1)).longValue()).window(TumblingProcessingTimeWindows.of(Time.seconds(10))).apply(new FlatJoinFunction<ShippingPriorityItem, Row, ShippingPriorityItem>() {
                    @Override
                    public void join(
                            ShippingPriorityItem first,
                            Row second,
                            Collector<ShippingPriorityItem> out) throws Exception {
                        first.setRevenue((double)((float)second.getFieldAs(5) * (1 - (float)second.getFieldAs(6))));
                        out.collect(first);
                    }
                });

        DataStream<ShippingPriorityItem> resultSum = result
                .keyBy(new KeySelector<ShippingPriorityItem, Object>() {
                    @Override
                    public Object getKey(ShippingPriorityItem x) throws Exception {
                        return Tuple3.of(x.getOrderkey(), x.getOrderdate(), x.getShippriority());
                    }
                })
                .reduce(new SumShippingPriorityItem());
        resultSum.print();
        sEnv.execute("1-QUERY");
    }
}
