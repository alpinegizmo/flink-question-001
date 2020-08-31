package org.example.prow;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.prow.wikimedia.Event;

public class App {
    final static OutputTag<Event> lateEvents = new OutputTag<Event>("late events", TypeInformation.of(Event.class));
    final static OutputTag<String> watermarks = new OutputTag<String>("watermarks", TypeInformation.of(String.class));

    public static void main(String[] args) throws Exception {

        // for convenience in debugging
        Configuration conf = new Configuration();
        conf.setString("heartbeat.timeout", "1000000");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        // complete dataset: https://dumps.wikimedia.org/other/mediawiki_history/2020-07/enwiki/2020-07.enwiki.2016-04.tsv.bz2
        final String fileNameInput = "file://" + System.getProperty("user.dir") + "/10000-lines.tsv.gz";

        BoundedOutOfOrdernessTimestampExtractor<Event> bounded = new BoundedOutOfOrdernessTimestampExtractor<Event>(Time.hours(1)) {
            @Override
            public long extractTimestamp(Event element) {
                return element.eventTimestamp.toEpochSecond() * 1000;
            }
        };

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(50);
        env.setParallelism(1);

        // with chaining disabled, the application runs correctly
//        env.disableOperatorChaining();

        SingleOutputStreamOperator<Event> results = env
                .readTextFile(fileNameInput)
                .map(value -> new Event(value))
                .assignTimestampsAndWatermarks(bounded)
                .keyBy(e -> e.eventEntity.toString())
                .process(new PlayWithEvents());

        results.getSideOutput(watermarks).printToErr();
//        results.getSideOutput(lateEvents).printToErr();
//        results.print();

        env.execute("FlinkWikipediaHistoryTopEditors");
    }

    private static class PlayWithEvents extends KeyedProcessFunction<String, Event, Event> {
        @Override
        public void processElement(Event event, Context ctx, Collector<Event> out) throws Exception {
            long t = ctx.timestamp();
            long w = ctx.timerService().currentWatermark();
            ctx.timerService().registerEventTimeTimer(w + 1);
            if (t < w) {
                ctx.output(lateEvents, event);
            } else {
                out.collect(event);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Event> out) throws Exception {
            long w = ctx.timerService().currentWatermark();

            ctx.output(watermarks, "-----------------------------------WM----------------------------------- " + w);
        }
    }
}
