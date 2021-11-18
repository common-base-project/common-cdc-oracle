package io.cdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Mustang Kong
 * @apiNote
 * @date 2021/11/14
 *
 * https://ververica.github.io/flink-cdc-connectors/release-2.1/index.html
 * https://github.com/czy006/FlinkClub
 *
 * mysql driver 一定要是8.0.21 8.0.16
 * <dependency>
 *             <groupId>mysql</groupId>
 *             <artifactId>mysql-connector-java</artifactId>
 *             <version>8.0.21</version>
 *         </dependency>
 */
public class MysqlCdcTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("10.236.101.13")
                .port(32450)
                .username("root")
                .password("1VfMyvwwrq")
                .databaseList("test")
                .tableList("test.user")
                //.serverId("5400")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        //MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
        //        .hostname("cdb-6ocyiznz.gz.tencentcdb.com")
        //        .port(10111)
        //        // set captured database
        //        .databaseList("test")
        //        // set captured table
        //        .tableList("test.missu_users")
        //        .username("root")
        //        .password("")
        //        .startupOptions(StartupOptions.initial())
        //        // converts SourceRecord to JSON String
        //        .deserializer(new JsonDebeziumDeserializationSchema())
        //        //.deserializer(new StringDebeziumDeserializationSchema())
        //        .build();

        env.enableCheckpointing(1000);
        env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL-Source")
                // set 4 parallel source tasks
                .setParallelism(4)
                .print()
                // use parallelism 1 for sink to keep message ordering
                .setParallelism(1);

        env.execute("Print MySQL Snapshot + Binlog");

    }
}
