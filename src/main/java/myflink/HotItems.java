package myflink;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class HotItems {
//    public static String s="";
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        URL url = HotItems.class.getClassLoader().getResource("UserBehavior.csv");
        Path filePath = Path.fromLocalFile(new File(url.toURI()));
        PojoTypeInfo<UserBehavior> pojoTypeInfo = (PojoTypeInfo<UserBehavior>) TypeExtractor.createTypeInfo(UserBehavior.class);
        String[] fieldOrder = new String[]{"userId", "itemId", "categoryId", "behavior", "timestamp"};
        PojoCsvInputFormat<UserBehavior> csvInput = new PojoCsvInputFormat<>(filePath, pojoTypeInfo, fieldOrder);
        //从文件中读取数据
        DataStream<UserBehavior> dataSource = env.createInput(csvInput, pojoTypeInfo);
        //设置环境的时间为事件发生的时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置数据按照时间排序
        DataStream<UserBehavior> timeData = dataSource
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior userBehavior) {
                        return userBehavior.timestamp * 1000;
                    }
                });
        //过滤出来行为是“点击”的事件
        DataStream<UserBehavior> pvData = timeData
                .filter(new FilterFunction<UserBehavior>() {
                    @Override
                    public boolean filter(UserBehavior userBehavior) throws Exception {
                        return userBehavior.behavior.equals("pv");
                    }
                });
        //设置滑动窗口大小为60分钟，每过5分钟滑动一次，同时统计一下每个商定id在窗口内的点击数
        DataStream<ItemViewCount> windowData = timeData
                .keyBy("itemId")
                .timeWindow(Time.minutes(60), Time.minutes(5))
                .aggregate(new CountAgg(), new WindowResultFunction());
        //筛选出每个窗口前n个商品的id及点击数
        DataStream<String> topItem = windowData
                .keyBy("windowEnd")
                .process(new TopNHotItems(3));
        //数据流sink，输出到控制台
        topItem.print();
        //活动开始
        env.execute("Hot Items Job");
//        System.out.println("******************************************");
//        System.out.println(s);
    }
//计算窗口中每个商品ID所点击的次数
    public static class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long acc) {
            return acc + 1;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long acc1, Long acc2) {
            return acc1 + acc2;
        }
    }
    public static class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {

        @Override
        public void apply(
                Tuple key,  // 窗口的主键，即 itemId
                TimeWindow window,  // 窗口
                Iterable<Long> aggregateResult, // 聚合函数的结果，即 count 值
                Collector<ItemViewCount> collector  // 输出类型为 ItemViewCount
        ) throws Exception {
            Long itemId = ((Tuple1<Long>) key).f0;
            Long count = aggregateResult.iterator().next();
            collector.collect(ItemViewCount.of(itemId, window.getEnd(), count));
        }
    }
// 商品点击量(窗口操作的输出类型)
    public static class ItemViewCount {
        public long itemId;     // 商品ID
        public long windowEnd;  // 窗口结束时间戳
        public long viewCount;  // 商品的点击量

        public static ItemViewCount of(long itemId, long windowEnd, long viewCount) {
            ItemViewCount result = new ItemViewCount();
            result.itemId = itemId;
            result.windowEnd = windowEnd;
            result.viewCount = viewCount;
            return result;
        }
    }
//列出每个窗口的前n个商品
    public static class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {
        private final int topSize;

        public TopNHotItems(int topSize) {
            this.topSize = topSize;
        }
        private ListState<ItemViewCount> itemState;
        @Override
        public void open(Configuration configuration)throws Exception{
            super.open(configuration);
            ListStateDescriptor<ItemViewCount> itemsStateDesc = new ListStateDescriptor<>(
                    "itemState-state",
                    ItemViewCount.class);
            itemState=getRuntimeContext().getListState(itemsStateDesc);
        }
        @Override
        public void processElement(ItemViewCount input, Context context, Collector<String> collector) throws Exception {
            itemState.add(input);
            context.timerService().registerEventTimeTimer(input.windowEnd+1);
        }
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception{
            List<ItemViewCount> allItems=new ArrayList<ItemViewCount>();
            for (ItemViewCount item:itemState.get()
                 ) {
                allItems.add(item);
            }
            itemState.clear();
            allItems.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1,ItemViewCount o2) {
                    return (int) (o1.viewCount - o2.viewCount);
                }
            });
            StringBuilder result = new StringBuilder();
            result.append("----------------------------\n");
            for (int i=0;i<topSize;i++){
                ItemViewCount currentItem=allItems.get(i);
                result.append("No").append(i).append(":")
                        .append("  商品ID=").append(currentItem.itemId)
                        .append("  浏览量=").append(currentItem.viewCount)
                        .append("\n");
            }
            result.append("----------------------------\n");
//            s+=result;
            out.collect(result.toString());
        }
    }
}
