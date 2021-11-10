package io.cdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MySqlSourceExample {

    public static void main(String[] args) throws Exception {

        //1.获取Flink 执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        // set 4 parallel source tasks
//        env.setParallelism(1);

        //1.1 开启CK
//        env.enableCheckpointing(5000);
//        env.getCheckpointConfig().setCheckpointTimeout(10000);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/cdc-test/ck"));

        //2.通过FlinkCDC构建SourceFunction
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("10.236.101.13")
                .port(32450)
                .username("root")
                .password("1VfMyvwwrq")
                .databaseList("db_sau")
                .tableList("db_sau.t_comment_info")
                .deserializer(new JsonDebeziumDeserializationSchema())
//                .deserializer(new CustomerDeserializationSchema())
//                .startupOptions(StartupOptions.initial())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint
        env.enableCheckpointing(3000);

        env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // set 4 parallel source tasks
                // 设置4个并行源任务
                .setParallelism(2)
                // 对接收器使用并行度 1 以保持消息排序
                // use parallelism 1 for sink to keep message ordering
                .print().setParallelism(1);

        env.execute("Print MySQL Snapshot + Binlog");

    }

}
