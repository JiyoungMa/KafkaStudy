package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HdfsSinkApplication {
    private final static Logger logger = LoggerFactory.getLogger(HdfsSinkApplication.class);

    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
    private final static String TOPIC_NAME = "select-color";
    private final static String GROUP_ID = "color-hdfs-save-consumer-group";
    private final static int CONSUMER_COUNT = 3;
    private final static List<ConsumerWorker> workers = new ArrayList<>();

    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new ShutdownThread());

        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //컨슈머 스레드를 스레드 풀로 관리하기 위해 생성
        ExecutorService executorService = Executors.newCachedThreadPool();
        //컨슈머 스레드를 CONSUMER_COUNT 만큼 생성하고 생성된 컨슈머 스레드 인스턴스들을 묶음으로 관리하기 위해 리스트에 추가
        for(int i = 0; i<CONSUMER_COUNT; i++){
            workers.add(new ConsumerWorker(configs, TOPIC_NAME, i));
        }

        // 컨슈머 스레드 인스턴스들을 스레드 풀에 포함시켜 실행
        workers.forEach(executorService::execute);
    }

    static class ShutdownThread extends Thread{
        @Override
        public void run() {
            logger.info("Shutdown hook");
            workers.forEach(ConsumerWorker::stopAndWakeup);
        }
    }
}
