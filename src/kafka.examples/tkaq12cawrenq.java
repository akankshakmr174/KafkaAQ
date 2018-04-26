package kafka.examples;/* $Header: tkmain_8/tkaq/src/tkaq12cawrenq.java ichokshi_bug-27402192/1 2018/02/19 07:06:01 ichokshi Exp $ */

/* Copyright (c) 2018, Oracle and/or its affiliates. All rights reserved.*/

/*
   DESCRIPTION
     Java program to do Key Based Enqueue. I.e. set 'key' as correlation id.
     10 Keys are predefiend in an array which are Key_1, Key_2 .. Key_10
     argv[7] and argv[8] are start and end index of this array.
     argv[9] is number of message of each key to enqueue.
     Thus if argv[7], argv[8], argv[9] = 2, 6, 15 then program will enqueue
     15 messages each for Key_3, Key_4, Key_5, Key_6.
     Queue Name is JMS_TEXT_T2. 
    
   PRIVATE CLASSES
    <list of private classes defined - with one-line descriptions>

   NOTES
    <other useful comments, qualifications, etc.>

   MODIFIED    (MM/DD/YY)
    ichokshi    02/19/18 - Creation
 */

/**
 *  @version $Header: tkmain_8/tkaq/src/tkaq12cawrenq.java ichokshi_bug-27402192/1 2018/02/19 07:06:01 ichokshi Exp $
 *  @author  ichokshi
 *  @since   release specific (what release of product did this appear in)
 */

import oracle.AQ.*;
import oracle.jms.*;
import javax.jms.*;
import java.lang.*;
import java.util.*;
import java.sql.*;


public class tkaq12cawrenq
{
		TopicConnectionFactory tcf;
		TopicConnection tCon;
		TopicSession tSess;
		Topic topic;
		TopicPublisher tPublisher;
	
		String url;
		String oracleSid;
		String host;
		String port;
		String service;
		String driver;
		String username;
		String password;
		String queueName;
    String shardKey;
    
    static String[] shardKeys = new String[100];
    static {
      for(int i=0;i<100;i++)
        shardKeys[i] = "Key_"+(i+1);
    }

    int kStart =0;
    int kEnd =0;
		int noOfMsg;
    boolean isCdb = false;
    boolean commitTxn = true;
  	
		public static void main(String argv[])
		{
		
		  tkaq12cawrenq enqMsg = new tkaq12cawrenq();
			String msg = "";
      boolean isMixed = false;
			try
			{
        enqMsg.parseArgs(argv); 
				enqMsg.setUp();
        enqMsg.commitTxn = true;

        String key;
        int j,i;
        int msgNum = 0;
                long start=System.currentTimeMillis();
        for(i=0; i<enqMsg.noOfMsg; i++)
        {
          for(j=enqMsg.kStart; j<enqMsg.kEnd; j++)
          {

            key = enqMsg.shardKeys[j];
            msgNum++;
     				TextMessage txtMsg = enqMsg.createTextMsg("This is Test " +msgNum, key);
    				enqMsg.enqueue(txtMsg);
            if(enqMsg.commitTxn)
            {
              enqMsg.tSess.commit();
            }

          }
          System.out.println("Enqueued "+ msgNum +" messages ");
        }
                long end=System.currentTimeMillis();
                long total=start-end;
                System.out.println("Total in s"+total/1000);
			}
			catch(Exception e)
			{
				System.out.println("Exception in main " + e);
				e.printStackTrace();
			}
      finally {
           try {
              if(enqMsg != null)
                 enqMsg.close();
           } catch(Exception e) {
                System.out.println("Exception while closing session " + e);
                return;
          }
          System.out.println("Enqueue Complete");
      }
 	  } 
  /* SID HOST PORT DRIVER CDB? USERNAME PASSWORD KEYSTART KEYEND #MESSAGES */
  private void parseArgs(String argv[])
  {
    oracleSid = argv[0];
    host = argv[1];
    port = argv[2];
    driver = argv[3];
    setCdb(argv[4]);
    username = argv[5];
    password = argv[6];
    try {
      kStart = Integer.parseInt(argv[7]);
      kEnd   = Integer.parseInt(argv[8]);
      noOfMsg = Integer.parseInt(argv[9]);
    } catch(Exception e ) {kStart=0;kEnd=1;noOfMsg = 1 ;}
    
    Random rand = new Random();
    int r = rand.nextInt(50)+1;
   if(r%2==0) { commitTxn = false; }
  }


		public void close() throws Exception
		{
      try
      {
       System.out.println("Commiting in 1 sec");
       Thread.sleep(1000);
      }
      catch(Exception e){}

      tSess.commit();
			tCon.close();
		}

		private void setUp()  throws Exception
		{
      String url;
      queueName = "JMS_TEXT_T1";
      service = oracleSid;
      if (isCdb)
       url = tkaqjutl.createURL(oracleSid, host, port,driver,1);
      else
        url = tkaqjutl.createURL(oracleSid, host, port, driver, service);

      System.out.println("Trying to connect to url " +url);
			Properties prop = new Properties();
			prop.setProperty("user",username);
			prop.setProperty("password",password);
		
			tcf = (AQjmsTopicConnectionFactory) AQjmsFactory.getTopicConnectionFactory(url,prop);
			tCon = tcf.createTopicConnection();
			tSess = tCon.createTopicSession(true,Session.AUTO_ACKNOWLEDGE);
			topic = ((AQjmsSession)tSess).getTopic(username.toUpperCase(), queueName.toUpperCase());
			
			tPublisher = tSess.createPublisher(topic);
			tCon.start();
			
		}	

		public TextMessage createTextMsg(String text, String key) throws Exception
		{
				TextMessage txtMsg = tSess.createTextMessage();
        txtMsg.setJMSCorrelationID(key);
				txtMsg.setText(text);
				return txtMsg;
		}	

		public void enqueue(TextMessage txtMsg) throws Exception
		{
			tPublisher.publish(topic,txtMsg, DeliveryMode.PERSISTENT,5,AQjmsConstants.EXPIRATION_NEVER);
		}
    
    public void  setCdb(String isCdbStr)
    {
      if(isCdbStr == null) {
       isCdb = false;
        return;
      }
      isCdb = isCdbStr.equalsIgnoreCase("TRUE")?true:false;
   }
}
