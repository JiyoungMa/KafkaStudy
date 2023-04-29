package org.example;

import com.google.gson.Gson;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.example.config.ElasticSearchSinkConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

// 실질적인 엘라스틱서치 적재 로직이 들어가는 클래스
public class ElasticSearchSinkTask extends SinkTask {
    private final Logger logger = LoggerFactory.getLogger(ElasticSearchSinkTask.class);

    private ElasticSearchSinkConnectorConfig config;
    private RestHighLevelClient esClient;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        try{
            config = new ElasticSearchSinkConnectorConfig(props);
        } catch (ConfigException e){
            throw new ConnectException(e.getMessage(), e);
        }

        // 엘라스틱서치에 적재하기 위해 ResstHighLevelClient 인스턴스르 생성
        esClient = new RestHighLevelClient(RestClient.builder(new HttpHost(config.getString(config.ES_CLUSTER_HOST),config.getInt(config.ES_CLUSTER_PORT))));
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        // 레코드가 들어올 경우 엘라스틱 서치로 전송하기 위한 BulkRequest 인스턴스 생성
        // BulkRequest는 1개 이상의 데이터들을 묶음으로 엘라스틱서치로 전송할 때 사용
        if(records.size() > 0){
            BulkRequest bulkRequest = new BulkRequest();
            for(SinkRecord record: records){
                Gson gson = new Gson();
                Map map = gson.fromJson(record.value().toString(), Map.class);
                bulkRequest.add(new IndexRequest(config.getString(config.ES_INDEX)).source(map, XContentType.JSON));
                logger.info("record : {}", record.value());
            }

            // bulkAsync() 메서드를 사용하면 데이터를 전송하고 난 뒤, 결과를 비동기로 받아서 확인할 수 있다.
            esClient.bulkAsync(bulkRequest, RequestOptions.DEFAULT, new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse bulkResponse) {
                    if(bulkResponse.hasFailures()){
                        logger.error(bulkResponse.buildFailureMessage());
                    }else{
                        logger.info("bulk save success");
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error(e.getMessage(), e);
                }
            });
        }
    }

    // flush 메서드는 일정 주기마다 호출된다.
    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets){
        logger.info("flush");
    }

    // 커넥터가 종료될 경우 엘라스틱서치와 연동하는 esClient 변수를 안전하게 종료한다.
    @Override
    public void stop() {
        try{
            esClient.close();
        }catch (IOException e){
            logger.info(e.getMessage(), e);
        }
    }
}
