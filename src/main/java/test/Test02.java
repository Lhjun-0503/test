package test;



import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import redis.clients.jedis.Jedis;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Test02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999);

        //转JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = dataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] values = value.split(" ");

                WaterSensor waterSensor = new WaterSensor(values[0], Long.parseLong(values[1]), Integer.parseInt(values[2]));

                return waterSensor;
            }
        });

        //获取当前时间
        long ts = System.currentTimeMillis();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        String date = sdf.format(new Date(ts));

        SingleOutputStreamOperator<TestSensor> testWaterSensorWithIncreaseIdDStream = waterSensorDStream.map(new RichMapFunction<WaterSensor, TestSensor>() {

            //定义jedis连接
            Jedis jedis;

            @Override
            public void open(Configuration parameters) throws Exception {
                //创建redis连接
                jedis = new Jedis("localhost", 6379);

            }

            @Override
            public TestSensor map(WaterSensor waterSensor) throws Exception {

                //redisKey
                String redisKey = date + "_" + waterSensor.getId();

                //查询redis,获取自增id
                String increaseId = jedis.get(redisKey);

                if (increaseId == null) {

                    increaseId = 1 + "";

                } else {
                    increaseId = (Integer.parseInt(increaseId) + 1) + "";
                }

                //更新redis
                jedis.set(redisKey, increaseId);

                //创建新的JavaBean，保存自增id
                TestSensor testSensor = new TestSensor();

                //补全新的JavaBean
                testSensor.setId(waterSensor.getId());
                testSensor.setTs(waterSensor.getTs());
                testSensor.setVc(waterSensor.getVc());

                //自增id
                testSensor.setIncreaseId(Integer.parseInt(increaseId));

                return testSensor;
            }
        });


        testWaterSensorWithIncreaseIdDStream.print();

        env.execute();

    }
}
