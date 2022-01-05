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

public class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 9999);

        //转JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = dataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] values = value.split(" ");

                WaterSensor waterSensor = new WaterSensor(values[0], Long.parseLong(values[1]), Integer.parseInt(values[2]));

                return waterSensor;
            }
        });

        //按id keyBy
        KeyedStream<WaterSensor, String> keyedStream = waterSensorDStream.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });

        SingleOutputStreamOperator<TestSensor> testWaterSensorWithIncreaseIdDStream = keyedStream.map(new RichMapFunction<WaterSensor, TestSensor>() {
            //定义状态
            ValueState<Integer> valueState;


            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化状态
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("valueState", Integer.class));
            }

            @Override
            public TestSensor map(WaterSensor waterSensor) throws Exception {



                //判断状态的值是否为null，为null说明该条数据第一次进入计算，自增id设为1
                if (valueState.value() == null) {
                    //更新状态，状态值设为1
                    valueState.update(1);

                } else {
                    //状态的值自增

                    Integer increaseId = valueState.value();

                    increaseId += 1;

                    valueState.update(increaseId);

                }

                //获取状态的值
                Integer increaseId = valueState.value();

                //创建新的JavaBean，保存自增id
                TestSensor testSensor = new TestSensor();

                //补全新的JavaBean
                testSensor.setId(waterSensor.getId());
                testSensor.setTs(waterSensor.getTs());
                testSensor.setVc(waterSensor.getVc());

                //自增id
                testSensor.setIncreaseId(increaseId);

                return testSensor;


            }
        });

        testWaterSensorWithIncreaseIdDStream.print();

        env.execute();
    }
}
