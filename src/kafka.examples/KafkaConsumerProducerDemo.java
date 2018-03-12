package kafka.examples;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;


import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaAQProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaAQConsumer;

import java.util.*;

class KafkaTopic {
    ZkClient zkClient = null;
    ZkUtils zkUtils = null;
    String topicName=null;
    public KafkaTopic(int partition, int replication){
        String zookeeperHosts = "den01syu.us.oracle.com:2181"; // If multiple zookeeper then -> String zookeeperHosts = "192.168.20.1:2181,192.168.20.2:2181";
        int sessionTimeOutInMs = 15 * 1000; // 15 secs
        int connectionTimeOutInMs = 10 * 1000; // 10 secs

        zkClient = new ZkClient(zookeeperHosts, sessionTimeOutInMs, connectionTimeOutInMs, ZKStringSerializer$.MODULE$);
        zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperHosts), false);

        topicName = "testTopic2";
        Properties topicConfiguration = new Properties();

        AdminUtils.createTopic(zkUtils, topicName, partition, replication, topicConfiguration, RackAwareMode.Enforced$.MODULE$);
        //Map<ConfigResource,Config> alterMap=new HashMap<ConfigResource,Config>();
        //AdminClient.alterConfigs(alterMap.put(ConfigResource(BROKER,resName),ConfigEntry(ConfigName, value) )); //introduce any config in the ConfigName variable with the value

        //Map<String,NewPartitions> map=new HashMap<String,NewPartitions>(); NewPartitions class only in Kafka 1.0
        //AdminClient.createPartitions(map.put(topicName,NewPartitions(6)); //will only work with Kafka 1.0
        //zkClient.close();
    }


    public String returnTopicName(){
        return topicName;
    }
}

class Producer extends Thread {
    private final KafkaAQProducer<Integer, String> producer;
    private final String topic;
    private final Boolean isAsync;

    public Producer(String topic, Boolean isAsync) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "den01syu.us.oracle.com:9092");
        props.put("oracle.host", "slc06cjr.us.oracle.com:1521");
        props.put("oracle.sid", "jms1");
        props.put("oracle.service", "jms1.regress.rdbms.dev.us.oracle.com");
        props.put("oracle.user", "aq");
        props.put("oracle.password", "aq");
        props.put("client.id", "DemoProducer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaAQProducer<Integer, String>(props);
        //producer = new KafkaAQProducer<Integer, String>(props);
        this.topic = topic;
        this.isAsync = isAsync;
        System.out.println("Kafka Producer initialized");
    }

    public void run() {
        int messageNo = 1;
        while (messageNo < 10) {
            System.out.println("Message number " + messageNo);
            String messageStr = "Message_" + messageNo;
            long startTime = System.currentTimeMillis();
            if (isAsync) { // Send asynchronously
                producer.send(new ProducerRecord<>(topic,
                        messageNo,
                        messageStr), new DemoCallBack(startTime, messageNo, messageStr));
            } else { // Send synchronously
                System.out.println("Sending message");
                try {

                    producer.send(new ProducerRecord<>(topic,
                            messageNo,
                            messageStr)).get();
                    System.out.println("Sent message: (" + messageNo + ", " + messageStr + ")");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            ++messageNo;
        }
    }
}

class DemoCallBack implements Callback {

    private final long startTime;
    private final int key;
    private final String message;

    public DemoCallBack(long startTime, int key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    /**
     * A callback method the user can implement to provide asynchronous handling of request completion. This method will
     * be called when the record sent to the server has been acknowledged. Exactly one of the arguments will be
     * non-null.
     *
     * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). Null if an error
     *                  occurred.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     */
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            System.out.println(
                    "message(" + key + ", " + message + ") sent to partition(" + metadata.partition() +
                            "), " +
                            "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
        } else {
            exception.printStackTrace();
        }
    }
}
class Consumer extends Thread{
    private final KafkaAQConsumer<Object, Object> consumer;
    private final String topic;

    public Consumer(String topic) {
        //super("KafkaConsumerExample", false);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "den01syu.us.oracle.com:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
        props.put("oracle.host", "slc06cjr.us.oracle.com:1521");
        props.put("oracle.sid", "jms1");
        props.put("oracle.service", "jms1.regress.rdbms.dev.us.oracle.com");
        props.put("oracle.user", "aq");
        props.put("oracle.password", "aq");
        //props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        //props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        //props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaAQConsumer<>(props);
        this.topic = topic;
        System.out.println("Consumer Initialized");
    }

    @Override
    public void run() {
        boolean cflag=true;
        consumer.subscribe(Collections.singletonList(this.topic));
        //consumer.assign(Collections.singletonList(new TopicPartition(KafkaProperties.TOPIC,0)));

        try {
            while (cflag) {
                System.out.println("I am here");
                ConsumerRecords<Object, Object> records = consumer.poll(1000);
                for (ConsumerRecord<Object, Object> record : records)
                {
                    System.out.println("topic = "+record.topic()+"partition = "+record.partition()+"offset = "+record.offset()+"message= "+record.key());
                    cflag=false;

                }
            }
        } finally {
            consumer.close();
        }
    }


}

class KafkaProperties {

    public static final String TOPIC = "topic1";
    public static final String KAFKA_SERVER_URL = "den01syu.us.oracle.com";
    public static final String KAFKA_SERVER_PORT = "9092";
    public static final int KAFKA_PRODUCER_BUFFER_SIZE = 64 * 1024;
    public static final int CONNECTION_TIMEOUT = 100000;
    public static final String TOPIC2 = "topic2";
    public static final String TOPIC3 = "topic3";
    public static final String CLIENT_ID = "SimpleConsumerDemoClient";

    private KafkaProperties() {}

}


/**
 * Created by akankkum on 12/1/17.
 */
public class KafkaConsumerProducerDemo {
    public static void main(String[] args) {
        //KafkaTopic kTopic=new KafkaTopic(1,1);
        //String ktopic=kTopic.returnTopicName();

        System.out.println("Starting Kafka Client");
        boolean isAsync = false;
        System.out.println("Starting Producer "+isAsync);
        long startTime=System.nanoTime();
        try {
            Producer producerThread = new Producer(KafkaProperties.TOPIC, isAsync);
            producerThread.start();

            producerThread.join();
        } catch(Exception e ) {
            System.out.println("Exception from Main " +e);
            e.printStackTrace();
        }
        long endTime = System.nanoTime();
        long totalTime =endTime-startTime;
        System.out.println(totalTime);
        System.out.println("Demo Ends");
    /*    System.out.println("Starting Consumer");
        Consumer consumerThread = new Consumer(KafkaProperties.TOPIC);
        consumerThread.start();*/

    }
}