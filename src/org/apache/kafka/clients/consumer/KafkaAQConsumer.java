package org.apache.kafka.clients.consumer;

import java.lang.IllegalStateException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import oracle.jms.*;
import org.apache.kafka.clients.consumer.ConsumerAQRecord;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.internals.*;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.*;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.*;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import javax.jms.*;


    public class KafkaAQConsumer<K, V> extends KafkaConsumer<K, V> {

        /*
         * AQjmsSession, AQjmsConnection, AQjmsPublisher
         */

        TopicConnectionFactory tcf;
        TopicConnection tCon;
        TopicSession tSess;
        Topic topic;
        TopicSubscriber tSubs;
        String oracleUrl;
        String user;
        boolean isStarted = false;
        java.sql.Connection dbConn = null;
        TextMessage msg = null;
        int numMsgs = 10;

        public KafkaAQConsumer(Properties props) {
            super();
            //super(props);
            String sid = props.getProperty("oracle.sid");
            String hostPort = props.getProperty("oracle.host");
            String service = props.getProperty("oracle.service");
            user = props.getProperty("oracle.user");
            String pass = props.getProperty("oracle.password");

            StringTokenizer stn = new StringTokenizer(hostPort, ":");
            String host = stn.nextToken();
            String port = stn.nextToken();
            oracleUrl = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(host=" + host + ")" +
                    "(port=" + port + "))(CONNECT_DATA=(INSTANCE_NAME=" + sid + ")" +
                    "(SERVICE_NAME=" + service + ")))";
            System.out.println("Connecting to url " + oracleUrl);
            Properties oraProp = new Properties();
            oraProp.setProperty("user", user);
            oraProp.setProperty("password", pass);
            try {
                tcf = (AQjmsTopicConnectionFactory) AQjmsFactory.getTopicConnectionFactory(oracleUrl, oraProp);
                tCon = tcf.createTopicConnection();
                tSess = tCon.createTopicSession(true, Session.CLIENT_ACKNOWLEDGE);
                isStarted = false;
                dbConn = ((AQjmsSession) tSess).getDBConnection();

            } catch (Exception e) {
                System.out.println("Exception while creating connection " + e);
                e.printStackTrace();
            }
        }


        public Set<TopicPartition> assignment() {
            return null;
        }


        public Set<String> subscription() {
            return null;
        }

        @Override
        public void subscribe(Collection<String> topics) {
            Iterator<String> topicIterate = topics.iterator();
            while (topicIterate.hasNext()) {
                try {
                    String topicNow = topicIterate.next();
                    System.out.println("Topic Now = " + topicNow);
                    topic = ((AQjmsSession) tSess).getTopic(user, topicNow);
                    tSubs = ((AQjmsSession) tSess).createDurableSubscriber(topic, "S1");
                } catch (Exception e) {
                    System.out.println("Exception while creation subscription!" + e);
                    e.printStackTrace();
                }
            }
        }


        public void unsubscribe() {

        }

        @Override
        public ConsumerRecords<K, V> poll(long timeout) {
            String key = null;
            String val = null;

         ConsumerAQRecord<K, V> dummyCon = new ConsumerAQRecord<>();
            try {
                if (!isStarted) {
                    tCon.start();
                    isStarted = true;
                }

                do {
                    msg = (TextMessage) tSubs.receive(timeout);
                    key = msg.getJMSCorrelationID();
                    val = msg.getText();
                    System.out.println(key + " " + val);
                    dummyCon.retVal();
                    System.out.println(dummyCon);
                } while (msg != null);
            } catch (Exception e) {
                System.out.println("Cannot get messages!" + e);
            }

            return dummyCon;
        }


        @Override
        public void close() {

            try {
                if (tSess != null)
                    tSess.close();

                if (tCon != null)
                    tCon.close();

                isStarted = false;
            } catch (Exception e) {
                System.out.println("Exception while closing consumer! " + e);
                e.printStackTrace();
            }
        }


    }
