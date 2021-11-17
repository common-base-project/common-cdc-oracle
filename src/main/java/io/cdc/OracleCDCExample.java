package io.cdc;

import com.ververica.cdc.connectors.oracle.OracleSource;
import com.ververica.cdc.connectors.oracle.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * oracle jdbc drive：
 * https://debezium.io/releases/
 */
public class OracleCDCExample {

    public static void main(String[] args) throws Exception {
        //1.获取Flink 执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        //1.1 开启CK
//        env.enableCheckpointing(5000);
//        env.getCheckpointConfig().setCheckpointTimeout(10000);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/cdc-test/ck"));

        SourceFunction<String> sourceFunction = OracleSource.<String>builder()
                .hostname("10.236.101.15")
                .port(1521)
                // monitor XE database
                .database("XE")
                // monitor inventory schema
                .schemaList("inventory")
                // monitor products table
                .tableList("inventory.products")
                .username("flinkuser")
                .password("flinkpw")
                // converts SourceRecord to JSON String
                .deserializer(new JsonDebeziumDeserializationSchema())
                //.startupOptions(StartupOptions.initial())
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);
        env
                .addSource(sourceFunction)
                // use parallelism 1 for sink to keep message ordering
                .print().setParallelism(1);
        env.execute();


//        //2.通过FlinkCDC构建SourceFunction
//        SourceFunction<String> sourceFunction = OracleSource.<String>builder()
//                .hostname("172.20.254.14")
//                .port(1521)
//                .database("orcl") // monitor XE database
//                .schemaList("CQDX_JXGLXX")    // monitor inventory schema
//                .tableList("CQDX_JXGLXX.UNITIME_STUDENT") // monitor products table
//                .username("CQDX_JXGLXX")
//                .password("cquisse")
//                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
//                .build();
//
//        //3.数据打印
////        dataStreamSource.print();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.addSource(sourceFunction)
//                .print()
//                .setParallelism(1); // use parallelism 1 for sink to keep message ordering
//
//        //4.启动任务
//        env.execute("FlinkCDC");


    }

}
