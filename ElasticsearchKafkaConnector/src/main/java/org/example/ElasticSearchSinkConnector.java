package org.example;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.example.config.ElasticSearchSinkConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticSearchSinkConnector extends SinkConnector {
    private final Logger logger = LoggerFactory.getLogger(ElasticSearchSinkConnector.class);
    private Map<String,String> configProperties;

    // 커넥터가 최초로 실행될 때 실행되는 구문
    // 사용자로부터 설정값을 가져와서 ElasticSearchSinkConnectorConfig 인스턴스를 생성
    @Override
    public void start(Map<String, String> props) {
        this.configProperties = props;
        try{
            new ElasticSearchSinkConnectorConfig(props);
        }catch(ConfigException e){
            throw new ConnectException(e.getMessage(), e);
        }
    }

    // 커넥터를 실행했을 경우 태스크 역할을 할 클래스를 선언
    @Override
    public Class<? extends Task> taskClass() {
        return ElasticSearchSinkTask.class;
    }

    // 태스크별로 다른 설정값을 부여할 경우에는 여기에 로직을 넣을 수 있다
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String,String>> taskConfigs = new ArrayList<>();
        Map<String, String> taskProps = new HashMap<>();

        taskProps.putAll(configProperties);
        for(int i = 0; i < maxTasks; i++){
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

    @Override
    public void stop() {
        logger.info("Stop elasticsearch connector");
    }

    @Override
    public ConfigDef config() {
        return ElasticSearchSinkConnectorConfig.CONFIG;
    }

    @Override
    public String version() {
        return "1.0";
    }
}
