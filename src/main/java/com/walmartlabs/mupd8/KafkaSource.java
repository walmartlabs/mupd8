package com.walmartlabs.mupd8;

/**
 * Created by aanand1 on 14/01/14.
 */

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.*;
import kafka.javaapi.consumer.ConsumerConnector;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.walmartlabs.mupd8.application.Mupd8DataPair;
import com.walmartlabs.mupd8.application.Mupd8Source;

public class KafkaSource implements Mupd8Source  {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);

    ConsumerIterator<byte[],byte[]> consumerIterator = null;
    private String topic = null;
    private String zkConnect= null;
    private String groupId = null;
    private String key = null;
    ConsumerConnector consumerConnector = null;
    ObjectMapper objMapper = null;
    public KafkaSource(List<String> args){
        zkConnect = args.get(0);
        groupId = args.get(1);
        topic = args.get(2);
        key = args.get(3);
        consumerIterator = getIterator();
        objMapper=new ObjectMapper();
    }

    @Override
    public boolean hasNext() {
        try{
            return consumerIterator.hasNext();
        }catch(ConsumerTimeoutException e){
            LOG.error("Consumer timed out",e);
        }
        return false;
    }

    @Override
    public Mupd8DataPair getNextDataPair() {
        Mupd8DataPair ret = new Mupd8DataPair();
        byte[] msg = consumerIterator.next().message();
        ret._value = msg;
        ret._key = getValue(key,msg);
        return ret;
    }

    private String getValue(String key, byte[] msg){
        try{
            JsonNode jsonNode = objMapper.readTree(msg);
            String[] keyArr = key.split(":");
            for(String field: keyArr){
                jsonNode = jsonNode.get(field);
            }
            return jsonNode.asText();
        }catch(Exception e){
            LOG.info("Failed to get value:", e);
        }
        return null;
    }

    private ConsumerIterator<byte[],byte[]> getIterator(){
        consumerConnector = Consumer.createJavaConsumerConnector(getConsumerConfig());
        Map<String,Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> topicStreamMap = consumerConnector.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = topicStreamMap.get(topic);
        if(streams != null && streams.size() > 0){
            return streams.get(0).iterator();
        }
        return null;
    }


    private ConsumerConfig getConsumerConfig(){
        Properties props = new Properties();
        props.put("zookeeper.connect", zkConnect);
        props.put("group.id", groupId);
        props.put("consumer.timeout.ms", "-1");
        props.put("zookeeper.session.timeout.ms", "4000");
        props.put("zookeeper.sync.time.ms", "2000");
        props.put("auto.commit.interval.ms", "2000");
        props.put("rebalance.max.retries", "4");
        props.put("auto.offset.reset", "smallest");
        return new ConsumerConfig(props);
    }

    public void closeSource(){
        if(consumerConnector != null){
            consumerConnector.shutdown();
        }
    }
}