package org.apache.kafka.clients.producer;

import java.util.Properties;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import oracle.jms.*;
import javax.jms.*;

public class KafkaAQProducer<K,V>  extends KafkaProducer<K,V>{
	
   /*
    * AQjmsSession, AQjmsConnection, AQjmsPublisher
    */
	
   TopicConnectionFactory tcf;
   TopicConnection tCon;
   TopicSession tSess;
   Topic topic;
   TopicPublisher tPublisher;
   String oracleUrl;
   String user;
   boolean isStarted = false;
	
   public KafkaAQProducer(Properties props)
   {
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
	 oracleUrl = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(host="+host+")"+
			     "(port="+port+"))(CONNECT_DATA=(INSTANCE_NAME="+sid+")" +
			     "(SERVICE_NAME=" + service + ")))";
	 System.out.println("Connecting to url " + oracleUrl);
	Properties oraProp = new Properties();
	oraProp.setProperty("user", user);
	oraProp.setProperty("password", pass);
	try {
	  tcf = (AQjmsTopicConnectionFactory) AQjmsFactory.getTopicConnectionFactory(oracleUrl,oraProp);
	  tCon = tcf.createTopicConnection();
	  tSess = tCon.createTopicSession(true, 0);
	  isStarted = false;
	}catch(Exception e)
	{
		System.out.println("Exception while creating connection " + e);
		e.printStackTrace();
	}
   }
   
   @Override
   public Future<RecordMetadata> send(ProducerRecord<K, V> record)
   {
	   Future<RecordMetadata> dummyRecord = new AQDummyFuture();
	   try {
		   
		   if(topic == null)
		   {
		    topic = ((AQjmsSession)tSess).getTopic(user,record.topic());
		    tPublisher=tSess.createPublisher(topic);
		   }
		   
		   if(!isStarted)   {
			   tCon.start();
			   isStarted = true;
		   }
		   
		   TextMessage txtMsg = createTextMsg(record.value().toString(),record.key().toString());
		   
		   tPublisher.publish(topic, txtMsg);
		   
		   tSess.commit();
		   
	   	} catch(Exception e)  {
	   		System.out.println("Exception while Sending message to AQ" + e );
	   		e.printStackTrace();
	   	}
	   
	   return dummyRecord;
   }

   public TextMessage createTextMsg(String text, String key) throws Exception    {
       TextMessage txtMsg = tSess.createTextMessage();
       txtMsg.setJMSCorrelationID(key);
       txtMsg.setText(text);
       return txtMsg;
   }
   
   public  void close()
   {
	   try {
		   if(tSess !=null)
			   tSess.close();
		   
		   if(tCon!=null)
			   tCon.close();
		   
		   isStarted = false;
	   }catch(Exception e )
	   {
		   System.out.println("Exception while clossing producer " +e);
		   e.printStackTrace();
	   }
   }

   
   public final class AQDummyFuture implements Future<RecordMetadata>
   {
	    @Override
	    public boolean cancel(boolean interrupt) {
	        return false;
	    }

	    @Override
	    public boolean isCancelled() {
	        return false;
	    }

	    @Override
	    public RecordMetadata get() throws InterruptedException, ExecutionException {
	        return null;
	    }
	    

		@Override
		public RecordMetadata get(long arg0, TimeUnit arg1)
				throws InterruptedException, ExecutionException, TimeoutException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public boolean isDone() {
			// TODO Auto-generated method stub
			return false;
		}    
   }
}
