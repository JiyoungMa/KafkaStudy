package org.example;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ConsumerWorker implements Runnable{
    private final Logger logger = LoggerFactory.getLogger(ConsumerWorker.class);
    private static Map<Integer, List<String>> bufferString = new ConcurrentHashMap<>();
    private static Map<Integer,Long> currentFileOffset = new ConcurrentHashMap<>();

    private final static int FLUSH_RECORD_COUNT = 10;
    private Properties prop;
    private String topic;
    private String threadName;
    private KafkaConsumer<String, String> consumer;

    public ConsumerWorker(Properties prop, String topic, int number){
        logger.info("Generate ConsumerWorker");
        this.prop = prop;
        this.topic = topic;
        this.threadName = "consumer-thread-" + number;
    }

    @Override
    public void run() {
        Thread.currentThread().setName(threadName);
        consumer = new KafkaConsumer(prop);
        consumer.subscribe(Arrays.asList(topic));
        try{
            while(true){
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofSeconds(1));

                for(ConsumerRecord<String, String> record: records){
                    addHdfsFileBuffer(record);
                }
                saveBufferToHdfsFile(consumer.assignment());
            }
        }catch (WakeupException e){
            logger.warn("Wakeup consumer");
        }catch(Exception e){
            logger.error(e.getMessage(), e);
        }finally {
            consumer.close();
        }
    }

    private void saveBufferToHdfsFile(Set<TopicPartition> partitions) {
        partitions.forEach(p -> checkFlushCount(p.partition()));
    }

    // 파티션의 번호를 확인하여 flush를 수행할 만큼 레코드 개수가 찼는지 확인한다.
    private void checkFlushCount(int partitionNo) {
        if(bufferString.get(partitionNo) != null){
            if(bufferString.get(partitionNo).size() >FLUSH_RECORD_COUNT -1){
                save(partitionNo);
            }
        }
    }

    // 실질적인 HDFS 적재를 수행하는 메서드
    private void save(int partitionNo) {
        if(bufferString.get(partitionNo).size()>0){
            try{
                String fileName = "/data/color-" + partitionNo + "-" +
                        currentFileOffset.get(partitionNo) + ".log";
                Configuration configuration = new Configuration();
                configuration.set("fs.defaultFS", "hdfs://localhost:9000");
                FileSystem hdfsFileSystem = FileSystem.get(configuration);
                FSDataOutputStream fileOutputStream = hdfsFileSystem.create(new Path(fileName));
                fileOutputStream.writeBytes(StringUtils.join(bufferString.get(partitionNo), "\n"));
                fileOutputStream.close();

                bufferString.put(partitionNo, new ArrayList<>());
            }catch(Exception e){
                logger.error(e.getMessage(),e);
            }
        }
    }

    // 레코드를 받아서 메시지 값을 버퍼에 넣는 코드
    // currentFileOffset에서 오프셋 번호를 관리해서 이슈 발생 시, 파티션과 오프셋에 대한 정보를 알 수 있다.
    private void addHdfsFileBuffer(ConsumerRecord<String,String> record) {
        List<String> buffer = bufferString.getOrDefault(record.partition(), new ArrayList<>());
        buffer.add(record.value());
        bufferString.put(record.partition(), buffer);

        if(buffer.size() == 1){
            currentFileOffset.put(record.partition(), record.offset());
        }
    }

    //버퍼에 남아있는 모든 데이터를 저장하기 위한 메서드
    private void saveRemainBufferToHdfsFile(){
        bufferString.forEach((partitionNo,v) -> this.save(partitionNo));
    }

    // 셧다운 훅이 발생했을 때, 안전한 종료를 위해 consumer에 wakeup() 메서드를 호출한다.
    // 또한 남은 버퍼의 데이터를 모두 저장하기 위해 saveRemainBufferToHdfsFile 메서드도 호출한다.
    public void stopAndWakeup(){
        logger.info("stopAndWakeup");
        consumer.wakeup();
        saveRemainBufferToHdfsFile();
    }
}
