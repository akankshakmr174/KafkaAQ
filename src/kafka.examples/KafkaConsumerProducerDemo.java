package kafka.examples;

import kafka.admin.AdminClient;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;


import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.*;

class KafkaTopic {
    KafkaAdminClient topicClient;
    NewTopic t1=null;
    public KafkaTopic(int partition, short replication){

        Scanner s=new Scanner(System.in);
        System.out.println("Enter the name of the topic to be created");
        t1=new NewTopic(s.next(), partition,replication);
        Properties topicConfiguration = new Properties();
        topicConfiguration.put("oracle.host", "den01chp.us.oracle.com:1521");
        topicConfiguration.put("oracle.sid", "mydb");
        topicConfiguration.put("oracle.service", "mydb.regress.rdbms.dev.us.oracle.com");
        topicConfiguration.put("oracle.user", "aq");
        topicConfiguration.put("oracle.password", "aq");
        topicClient=new KafkaAdminClient(topicConfiguration);
        topicClient.createTopics(t1);

    }


    public String returnTopicName(){
        return t1.name() ;
    }
}

class Producer extends Thread {
    private final KafkaProducer<Integer, String> producer;
    private final String topic;
    private final Boolean isAsync;

    public Producer(String topic, Boolean isAsync) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "den01syu.us.oracle.com:9092");
        props.put("oracle.host", "den01chp.us.oracle.com:1521");
        props.put("oracle.sid", "mydb");
        props.put("oracle.service", "mydb.regress.rdbms.dev.us.oracle.com");
        props.put("oracle.user", "aq");
        props.put("oracle.password", "aq");
        props.put("client.id", "DemoProducer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<Integer, String>(props);
        //producer = new KafkaProducer<Integer, String>(props);
        this.topic = topic;
        this.isAsync = isAsync;
        System.out.println("Kafka Producer initialized");
    }

    public void run() {
        int messageNo = 1;
        System.out.println("Msage number " + messageNo);
        String messageStr = "Message_" + messageNo;
        try {
            producer.send(new ProducerRecord<>(topic,
                    messageNo,
                    messageStr)).get();
        } catch(Exception e)
        {
            System.out.println("Exception in first enqueue " + e);
            e.printStackTrace();
        }
        messageNo++;
        while (messageNo < 10001) {
            //System.out.println("Message number " + messageNo);
            messageStr = "Message_" + messageNo;
            long startTime = System.currentTimeMillis();
            if (isAsync) { // Send asynchronously
                producer.send(new ProducerRecord<>(topic,
                        messageNo,
                        messageStr), new DemoCallBack(startTime, messageNo, messageStr));
            } else { // Send synchronously
            //    System.out.println("Sending message");
                try {
                    //System.out.println("Message number " + messageNo);
                    messageStr = "Message_" + messageNo;
                    producer.send(new ProducerRecord<>(topic,
                            messageNo,
                            messageStr)).get();
                    if(messageNo % 1000 == 0)
                     System.out.println("Sent message: (" + messageNo + ", " + messageStr + ")");
                } catch (Exception e) {
                    System.out.println("Exception while sending message " + messageNo + " : " + e);
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
    private final KafkaConsumer<Object, Object> consumer;
    private final String topic;

    public Consumer(String topic) {
        //super("KafkaConsumerExample", false);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "den01syu.us.oracle.com:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
        props.put("oracle.host", "den01chp.us.oracle.com:1521");
        props.put("oracle.sid", "mydb");
        props.put("oracle.service", "mydb.regress.rdbms.dev.us.oracle.com");
        props.put("oracle.user", "aq");
        props.put("oracle.password", "aq");
        //props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        //props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        //props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<>(props);
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
                ConsumerRecords<Object, Object> records = consumer.poll(10000);
                for (ConsumerRecord<Object, Object> record : records)
                {
                    System.out.println("message= "+record.key()+"text"+record.value());
                    cflag=false;

                }
            }
        }catch(Exception e)
        {
            System.out.println("Queue out of messages" +e);
            //e.printStackTrace();
        }
        finally {
            consumer.close();
        }
    }


}

class KafkaProperties {

    public static final String TOPIC = "topic8";
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
        String ktopic;
        System.out.println("Enter the topic name:");

            KafkaTopic kTopic = new KafkaTopic(1, (short) 0);
            ktopic = kTopic.returnTopicName();
            System.out.println(ktopic);

        System.out.println("Starting Kafka Client");
        boolean isAsync = false;
       System.out.println("Starting Producer "+isAsync);
        long startTime=System.currentTimeMillis();
     try {
            Producer producerThread = new Producer(KafkaProperties.TOPIC, isAsync);
            producerThread.start();

            producerThread.join();
        } catch(Exception e ) {
            System.out.println("Exception from Producer Main " +e);
            //e.printStackTrace();
        }
        long endTime = System.currentTimeMillis();
        long totalTime =endTime-startTime;
  System.out.println(" Producer Total Time =  " +totalTime/1000 +  " secs ");
      System.out.println("Starting Consumer");

        try{
            Consumer consumerThread = new Consumer(KafkaProperties.TOPIC);
            startTime=System.currentTimeMillis();
            consumerThread.start();
            consumerThread.join();
            endTime=System.currentTimeMillis();
            totalTime=endTime-startTime;
            System.out.println(" Consumer Total Time =  " +totalTime/1000 +  " secs ");
        } catch (Exception e){
            System.out.println("Exception from Comsumer Main" +e);
            e.printStackTrace();
        }


        System.out.println("Demo Ends");

    }
}

/*Performance
Enqueue(KafkaAQ;10K messages): 79.91443708223333 minutes
Enqueue(Kafka;10K messages): 0.00776537318333 minutes
Dequeue(Kafka;10K messages):0.219689306666 minutes
Dequeue(KafkaAQ;10K messages):39.93 minutes
 */