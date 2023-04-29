package org.example;

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import java.util.Properties;

public class MetricStreams {

    private static KafkaStreams streams;

    public static void main(final String[] args) {

        Runtime.getRuntime().addShutdownHook(new ShutdownThread());

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "metric-streams-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> metrics = builder.stream("metric.all");
        // 메시지 값을 기준으로 분기처리하기 위해 MetricJsonUtils를 통해 Json 데이터에 적힌 메트릭 종류 값을 토대로 KStream을 두 갈래로 분기한다.
        KStream<String, String>[] metricBranch = metrics.branch(
                (key, value) -> MetricJsonUtils.getMetricName(value).equals("cpu"),
                (key, value) -> MetricJsonUtils.getMetricName(value).equals("memory")
        );

        metricBranch[0].to("metric.cpu");
        metricBranch[1].to("metric.memory");

        // 전체 CPU 사용량이 50%가 넘어갈 경우를 필터링 함
        KStream<String, String> filteredCpuMetric = metricBranch[0]
                .filter((key, value) -> MetricJsonUtils.getTotalCpuPercent(value) > 0.5);

        filteredCpuMetric.mapValues(value -> MetricJsonUtils.getHostTimestamp(value)).to("metric.cpu.alert");

        streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }

    static class ShutdownThread extends Thread {
        public void run() {
            streams.close();
        }
    }
}