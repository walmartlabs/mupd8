package com.walmartlabs.mupd8;

import java.io.File;
import com.google.common.io.Files;
import com.walmartlabs.mupd8.application.Mupd8DataPair;
import junit.framework.TestCase;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.javaapi.producer.Producer;

import java.util.List;
import java.util.ArrayList;
import java.util.Properties;

import kafka.server.KafkaServerStartable;
import kafka.utils.Time;
import org.apache.curator.test.TestingServer;

public class KafkaSourceTest extends TestCase {
    private int port = 9092;
    private int brokerId = 1;
    private TestingServer zkServer;
    private KafkaServerStartable kafkaServer;
    private KafkaSource kafkaSource;
    private File logDir;
    private String key = "Kf1";
    private String val = "data1";
    private String mesg = "{ \"" + key + "\" : \"" + val + "\" }";
    private String topic = "mupd8-test-topic";

    public void setUp() throws Exception{
        zkServer = new TestingServer();
        logDir = Files.createTempDir();
        kafkaServer = startKafkaServer(zkServer.getConnectString(), logDir.getAbsolutePath());
        produceMessage(topic,mesg);
        kafkaSource = getKafkaSource();
    }

    public void tearDown() throws Exception{
        kafkaSource.closeSource();
        kafkaServer.shutdown();
        zkServer.stop();
        logDir.delete();
    }

    public void testHasNext(){
        assertTrue("HasNext should return true when data", kafkaSource.hasNext());
        assertTrue("HasNext should not consume data",kafkaSource.hasNext());
        assertTrue("HasNext should not consume data",kafkaSource.hasNext());

    }

    public void testNext(){
        Mupd8DataPair mupd8DataPair = kafkaSource.getNextDataPair();
        assertEquals("Next should return the correct message",val,mupd8DataPair._key.toString());
    }

    private KafkaSource getKafkaSource(){
        List<String> argsList = new ArrayList<String>();
        argsList.add(zkServer.getConnectString());
        argsList.add("test-mupd8-consumer");
        argsList.add(topic);
        argsList.add(key);
        return new KafkaSource(argsList);
    }

    private KafkaServerStartable startKafkaServer(String zkConnect, String logDir){
        Properties props = new Properties();
        props.put("port", "9092");
        props.put("broker.id", "1");
        props.put("log.dir", logDir);
        props.put("zookeeper.connect",zkConnect);
        KafkaConfig kafkaConfig = new KafkaConfig(props);
        KafkaServerStartable server = new KafkaServerStartable(kafkaConfig);
        server.startup();
        return server;
    }

    private void produceMessage(String topic, String mesg){
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("producer.type", "async");
        props.put("batch.size", "1");
        ProducerConfig config = new ProducerConfig(props);
        Producer producer = new Producer<String, String>(config);
        producer.send(new KeyedMessage(topic,mesg));
    }

}
