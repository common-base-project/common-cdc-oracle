package io.cdc;

import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC {

    public static void main(String[] args) throws Exception {

        //1.获取Flink 执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        //1.1 开启CK
//        env.enableCheckpointing(5000);
//        env.getCheckpointConfig().setCheckpointTimeout(10000);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/cdc-test/ck"));

//        //2.通过FlinkCDC构建SourceFunction
//        DebeziumSourceFunction<String> sourceFunction = MySqlSource.<String>builder()
//                .hostname("10.236.101.13")
//                .port(32450)
//                .username("root")
//                .password("1VfMyvwwrq")
//                .databaseList("db_sau")
////                .tableList("db_sau.user_info")
////                .deserializer(new JsonDebeziumDeserializationSchema())
//                .deserializer(new StringDebeziumDeserializationSchema())
//                .startupOptions(StartupOptions.initial())
//                .build();
//        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);
//
//        //3.数据打印
//        dataStreamSource.print();
//
//        //4.启动任务
//        env.execute("FlinkCDC");


        com.ververica.cdc.connectors.mysql.source.MySqlSource<String> mySqlSource =
                com.ververica.cdc.connectors.mysql.source.MySqlSource.<String>builder()
                        .hostname("10.236.101.13")
                        .port(32450)
                        .databaseList("db_sau")
                        .tableList("db_sau.t_comment_info")
                        .username("root")
                        .password("1VfMyvwwrq")
//                        .serverId("5401-5404")
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .includeSchemaChanges(true) // output the schema changes as well
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // enable checkpoint
        env.enableCheckpointing(3000);
        // set the source parallelism to 4
        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySqlParallelSource")
//                .setParallelism(4)
                .print()
                .setParallelism(1);

        env.execute("Print MySQL Snapshot + Binlog");

    }

}
