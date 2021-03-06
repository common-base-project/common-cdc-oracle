package io.cdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MySqlSourceExample {

    public static void main(String[] args) throws Exception {

        //1.获取Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // set 4 parallel source tasks
        env.setParallelism(1);

        //1.1 开启CK
//        env.enableCheckpointing(5000);
//        env.getCheckpointConfig().setCheckpointTimeout(10000);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/cdc-test/ck"));

        //2.通过FlinkCDC构建SourceFunction
        MySqlSource<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("10.236.101.13")
                .port(32450)
                .username("root")
                .password("1VfMyvwwrq")
                .databaseList("db_sau")
//                .tableList("db_sau.t_comment_info")
//                .deserializer(new StringDebeziumDeserializationSchema())
                .deserializer(new JsonDebeziumDeserializationSchema())
//                .deserializer(new CustomerDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        // enable checkpoint
        env.enableCheckpointing(3000);

        env
                .fromSource(sourceFunction, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // set 4 parallel source tasks
                .setParallelism(4)
                //3.数据打印
                .print()
                .setParallelism(1); // use parallelism 1 for sink to keep message ordering

        //4.启动任务
        env.execute("Print MySQL Snapshot + Binlog");
    }

}
