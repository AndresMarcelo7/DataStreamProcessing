package edu.eci.isiot.flinkjobs;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.eci.isiot.flinkjobs.model.Temperature;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class KafkaDataStreamSimpleJob {


    public static void main(String[] args) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka:9092");
        properties.setProperty("zookeeper.connect", "zookeeper:2181");
        properties.setProperty("group.id", "test");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "kafka-hum",
                new SimpleStringSchema(),
                properties);

        kafkaConsumer.setStartFromEarliest();

        DataStream<String> kafkaTopicDataStream = env.addSource(kafkaConsumer);

        //TODO define datastream processing graph
        DataStream<Temperature> tempMap = kafkaTopicDataStream.map(new MapFunction<String, Temperature>() {
            @Override
            public Temperature map(String value) throws Exception {
                Temperature t = mapper.readValue(value,Temperature.class);
                return t;
            }
        });

        DataStream<Temperature> lowTemperature = tempMap.filter(new FilterFunction<Temperature>() {
            @Override
            public boolean filter(Temperature t) throws Exception {
                return t.getDegrees() <=20;
            }
        });
        DataStream<Temperature> highTemperature = tempMap.filter(new FilterFunction<Temperature>() {
            @Override
            public boolean filter(Temperature t) throws Exception {
                return t.getDegrees() > 20;
            }
        });
        /**
        PrintSinkFunction<DataStream<Temperature>> printSink = new PrintSinkFunction<>();
        printSink.setRuntimeContext(lowTemperature);
        printSink.invoke(lowTemperature);
         */

        SingleOutputStreamOperator<AlertMessage> highMap = highTemperature.map(new MapFunction<Temperature, AlertMessage>() {
            @Override
            public AlertMessage map(Temperature temperature) throws Exception {
                return new AlertMessage("felipemarcelo156@gmail.com",""+temperature.getDegrees());
            }
        });
        lowTemperature.addSink(new PrintSinkFunction<>());
        lowTemperature.print().setParallelism(1);
        System.out.println(lowTemperature.print());

        env.execute();
    }
}
