package org.apache.kafka.clients.producer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import oracle.jms.*;
import javax.jms.*;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.requests.ProduceRequest;


public class KafkaAQProducer<K,V>  {
	
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
	
   public KafkaAQProducer(ProducerConfig config)
   {
	 String sid = config.getString(ProducerConfig.ORACLE_SID);
	 String hostPort = config.getString(ProducerConfig.ORACLE_HOST); 
	 String service = config.getString(ProducerConfig.ORACLE_SERVICE);
	 user = config.getString(ProducerConfig.ORACLE_USER);
	 String pass = config.getString(ProducerConfig.ORACLE_PASSWORD);
	 
	 System.out.println("Kafka AQ Producer: sid " + sid + " hostPort " + hostPort  + " service " + service + " user " + user + " pass " + pass);
	 
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
	  System.out.println("Kafka AQ Producer Set");
	}catch(Exception e)
	{
		System.out.println("Exception while creating connection " + e);
		e.printStackTrace();
	}
   }

   public void publish(ClientRequest clientR) throws Exception
   {
	   ProduceRequest.Builder requestBuilder = (ProduceRequest.Builder)clientR.requestBuilder();
	   Map<TopicPartition, MemoryRecords> partitionMap = requestBuilder.getPartitionMap();
	   Set<TopicPartition> tpSet = partitionMap.keySet();
	   TopicPartition tpArray[] = new TopicPartition[tpSet.size()];
	   tpSet.toArray(tpArray);
	   String topicStr = "";
	   for( TopicPartition tpE : tpArray)
	   {
		  topicStr = tpE.topic();
		  System.out.println("Topic, Partition  of Topic Partition " + topicStr + " , " + tpE.partition());
	   }
	   Collection<MemoryRecords> mRecords =  partitionMap.values();
	   
//	   AQjmsBytesMessage aqjmsMessages[] = new AQjmsBytesMessage[mRecords.size()];
	   ArrayList<AQjmsBytesMessage> aqjmsMessages = new ArrayList<AQjmsBytesMessage>(10);
	   Iterator<MemoryRecords> mIter = mRecords.iterator();
	   MemoryRecords mRec;
	   byte dst[];
	   int cnt = 0;
	   while(mIter.hasNext())
	   {
		   mRec = mIter.next();
		   Iterator<MutableRecordBatch> mrbIter = mRec.batches().iterator();
		   
		   while(mrbIter.hasNext())
		   {
			  MutableRecordBatch mbr = mrbIter.next();
			  Iterator<Record> rIter  = mbr.iterator();
			  while(rIter.hasNext())
			  {
				  Record r = rIter.next();
				  ByteBuffer keyBuffer = r.key();
          		  ByteBuffer valueBuffer = r.value();
          		 try {
          			 String keyStr = new String(keyBuffer.getInt() +"");
          			 
          			 dst = new byte[valueBuffer.remaining()];
          			 valueBuffer.get(dst);
          			 
          			 System.out.println("Creating Bytes message: Key,Value " + keyStr +", " + Arrays.toString(dst));
          			 
          		     BytesMessage bmsg = createByteMsg(dst, keyStr);
          		     aqjmsMessages.add( (AQjmsBytesMessage)bmsg);
          		     //aqjmsMessages[cnt++] = (AQjmsBytesMessage)bmsg;
          			 
          		 }catch (Exception e) {System.out.println("Exception while getting buffers " + e); e.printStackTrace();}
			  }
		   }
	   }
	   
	   if(this.topic == null)
	   {
	    this.topic = ((AQjmsSession)tSess).getTopic(user,topicStr);
	    tPublisher=tSess.createPublisher(topic);
	   }
	   
	   if(!isStarted)   {
		   tCon.start();
		   isStarted = true;
		   System.out.println("connection Started ");
	   }
	   System.out.println("Publishing Messages ");
	   AQjmsBytesMessage aqjmsMessagesArr[] = new AQjmsBytesMessage[aqjmsMessages.size()];	   
	   aqjmsMessagesArr = aqjmsMessages.toArray(aqjmsMessagesArr);
	   ((AQjmsProducer) tPublisher).bulkSend(topic, aqjmsMessagesArr );
	   System.out.println("Message Sent, committing");
	   // ack=all  verify how that works
	   // check for commit based on time
	   tSess.commit();
   }
   
   public Future<RecordMetadata> send(ProducerRecord<K, V> record)
   {
	   Future<RecordMetadata> dummyRecord = new AQDummyFuture();
	   try {
		   System.out.println("Sending through AQ Producer");
		   if(topic == null)
		   {
		    topic = ((AQjmsSession)tSess).getTopic(user,record.topic());
		    tPublisher=tSess.createPublisher(topic);
		   }
		   
		   if(!isStarted)   {
			   tCon.start();
			   isStarted = true;
			   System.out.println("connection Started ");
		   }
		   
		   TextMessage txtMsg = createTextMsg(record.value().toString(),record.key().toString());
		   
		   tPublisher.publish(topic, txtMsg);
		   System.out.println("Message Sent");
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
   
   public AQjmsBytesMessage createByteMsg(byte[] body, String key) throws Exception    {
	   AQjmsBytesMessage byteMsg = (AQjmsBytesMessage) tSess.createBytesMessage();
       byteMsg.setJMSCorrelationID(key);
       byteMsg.writeBytes(body);
       return byteMsg;
   }
   
   public  void close()
   {
	   if(tSess != null)
	   {
		   synchronized(tSess)
		   {
			    try {   
     		     tSess.close();
     		     tSess = null;
	    		 tCon.close();
	    		 tCon = null;
	   
				}catch(Exception e )
				{
					System.out.println("Exception while clossing producer " +e);
					e.printStackTrace();
				}
		   }
	   }
	   this.isStarted = false;
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
