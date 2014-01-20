package com.walmartlabs.mupd8;

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

/* Implements Mupd8Source to consume data from Kafka
   KafkaSource requires
   1. Cluster zookeeper connect string (eg: "host1:2181,host2:2181)
   2. Consumer Group Name
   3. Kafka topic name
   4.[Optional]Key to be extracted from a json message
 */
public class KafkaSource implements Mupd8Source  {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);

    ConsumerIterator<byte[],byte[]> consumerIterator = null;
    private String topic = null;
    private String zkConnect= null;
    private String groupId = null;
    private String key = null;
    private long sleepTime = 1000;
    private int MAX_RETRY = 6;
    ConsumerConnector consumerConnector = null;
    ObjectMapper objMapper = null;

    public KafkaSource(List<String> args){
        zkConnect = args.get(0);
        groupId = args.get(1);
        topic = args.get(2);
        if(args.size() > 3){
            key = args.get(3);
        }
        initialize(0);
        objMapper=new ObjectMapper();
    }

    @Override
    public boolean hasNext() {
        return hasNext(0);
    }

    @Override
    public Mupd8DataPair getNextDataPair() {
         try {
            byte[] msg = consumerIterator.next().message();
            Mupd8DataPair ret = new Mupd8DataPair();
            ret._value = msg;
            ret._key = getValue(key,msg);
            return ret;
         } catch (Exception e) {
            LOG.error("Exception in getting next value from consumer stream", e);
        }
        return null;
    }

    public void closeSource(){
        if(consumerConnector != null){
            consumerConnector.shutdown();
        }
    }

    private String getValue(String key, byte[] msg){
        if(key != null){
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
        }
        return null;
    }

    private ConsumerIterator<byte[],byte[]> getIterator() {
        consumerConnector = Consumer.createJavaConsumerConnector(getConsumerConfig());
        Map<String,Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> topicStreamMap = consumerConnector.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = topicStreamMap.get(topic);
        if(streams != null && streams.size() > 0){
            return streams.get(0).iterator();
        }else{
            String mesg = "Couldn't create a consumer iterator";
            LOG.error(mesg);
        }
        return null;
    }

    //TODO Add option to configure consumer through a config file

    private ConsumerConfig getConsumerConfig(){
        Properties props = new Properties();
        props.put("zookeeper.connect", zkConnect);
        props.put("group.id", groupId);
        props.put("consumer.timeout.ms", "-1");
        props.put("zookeeper.session.timeout.ms", "10000");
        props.put("zookeeper.sync.time.ms", "4000");
        props.put("auto.commit.interval.ms", "4000");
        props.put("rebalance.max.retries", "4");
        props.put("auto.offset.reset", "smallest");
        return new ConsumerConfig(props);
    }

    private boolean hasNext(int retry){
        try {
            return consumerIterator.hasNext();
        } catch (Exception e) {
            String mesg = "Error thrown by consumer iterator has next";
            LOG.error(mesg, e);
            threadSleep(retry,mesg);
            return hasNext(retry+1);
        }
    }

    private void initialize(int retry){
        consumerIterator = getIterator();
        if(consumerIterator == null){
            threadSleep(retry, "Failed to initialize consumer iterator");
            initialize(retry + 1);
        }
    }

    private void threadSleep(int retry, String mesg){
        int retCount = (retry < MAX_RETRY) ? retry:MAX_RETRY;
        double sleepTimeinMs = sleepTime*Math.pow(2,retCount);
        LOG.error(mesg + " retrying.. in " + sleepTimeinMs + " retry count ");
        try {
            Thread.sleep((long) sleepTimeinMs);
        } catch (InterruptedException e) {
            LOG.error("Thread sleep interrupted",e);
        }
        if(retry == MAX_RETRY){
            initialize(0);
        }
    }

}