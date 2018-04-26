package kafka.examples;

import java.sql.*;
import java.io.*;
import java.util.*;
import java.util.Date;
import oracle.AQ.*;
import javax.jms.JMSException ;
import javax.jms.QueueConnectionFactory;
import javax.jms.TopicConnectionFactory;
import oracle.jms.*;
import oracle.sql.BLOB;
import oracle.sql.CLOB;
import java.util.Properties;

/**
 * A class to do misc. chores common to all tests
 * For example:
 * <pre>
 *    Connection conn = tkaqjutl.connect(args);
 * </pre>
 *
 * @author  JDBC members
 * @version %I%, %G%
 *
 *   MODIFIED    (MM/DD/YY)
 *   maba         03/21/14 - fix lrg 11577883
 *   skayoor      02/24/14 - Bug 18180897: Grant Privileges to admin users
 *   mjaiswal     07/05/13 - make corresponding dbcon lrg run-able
 *   vmukkara     05/07/13 - oci
 *   tbhosle      05/02/13 - add service arg to createURL
 *   abchitre     12/22/11 - changes for lrg 6566239
 *   rbolarra     02/09/11 - grant uts
 *   wezhou       09/12/07 - when connect as sysdba, try change_on_install
 *   qialiu       08/19/07 - populate msg
 *   jleinawe     07/05/05 - jdk1.5 remove ambiguous Queue references 
 *   jciminsk     12/12/03 - merge from RDBMS_MAIN_SOLARIS_031209 
 *   jleinawe     08/18/03 - selectQueuePrivileges with where clause 
 *   jleinawe     08/18/03 - selectQueuePrivileges with where clause 
 *   jleinawe     01/15/03 - update for dbjava/interop tests
 *   jleinawe     10/10/02 - printSortedEnumeration cleanup
 *   lzhao        10/07/02 - deprecation of oracle.jdbc.driver
 *   jleinawe     07/08/02 - update createUser permissions
 *   jleinawe     11/21/01 - cleanup selectUserMessages
 *   jleinawe     08/28/01 - add printSortedEnumeration 
 *   jleinawe     08/28/01 - add selectUserQueue for nt difs.
 *   jleinawe     07/13/01 - update internal database connection.
 *   rbhyrava     08/04/00 - add setDebug method
 *   rbhyrava     07/06/00 - add registerTopicConnection
 *   mrhodes      11/30/99 - adjust connectSys
 *   mrhodes      10/06/99 - make some methods public
 *   mrhodes      09/30/99 - replace tabs with spaces
 *   mrhodes      09/30/99 - replace tabs with spaces
 *   mrhodes      08/18/99 - add selectQueuePrivileges
 *   mrhodes      08/11/99 - add sort option to selectUserMessages
 *   bnainani     10/07/98 - fix bugs
 *   cwwong       05/19/97 - Add new method printCharArrayUnicode
 *   cwwong       09/19/97 - Overload createUser method with user provided 
 *                           name/password
 *   JDBC members          - Creation
 */


public class tkaqjutl
{
  private static String url;
  private static String ConnectDesc;   
  private static String ConnectIP;
  private static String ConnectPort;
  private static String ConnectSID;
  private static String ConnectID;
  private static String ConnectPass;


  private static boolean test;

  public static void main(String [] args)
    throws SQLException, ClassNotFoundException
  {
    System.out.println("tkaqjutl.java -- setup program");

    try 
    {
      
      if((args.length == 5) && (args[4].equalsIgnoreCase("sys")))
      {
	tkaqjutl.connectSys(args);
	System.out.println("Database connecting successfully as SYS");
      }
      else
      {
	tkaqjutl.connect(args, "scott", "tiger");
	System.out.println(getURL() + " and other " + getConnectIP() + " " +
			   getConnectPort() + " " + getConnectSID());
	System.out.println("Database connecting successfully.");
      }


    } 
    catch (ArrayIndexOutOfBoundsException ex) 
    {
      System.out.println ("Not enough parameters. Usage: java filename [SID] [HOST] [PORT] [DRIVER]\n" + "\tExample: java tkaqjutl orcl bnainani-pc 5521 thin");
    }
  }

  /**
   * Returns a Connection method with the arguments passed as
   * a string array.
   *
   * @param     args arguments used to construct the connection string
   * @return    a connection object specified by the connection string
   * @exception SQLException SQLException
   * @exception ClassNotFoundException ClassNotFoundException
   *
   */
  public static Connection connect(String[] args, String user, String passwd)
    throws SQLException, ClassNotFoundException
  {

    return connect(args[0], args[1], args[2], args[3], user, passwd);

  }
  

  /**
   * Returns a Connection method.
   *
   * @param     sid      oracle sid
   * @param     host     database host
   * @param     port     listening port number
   * @param     driver   oci7 or oci8 or thin
   * @param     username username
   * @param     passwd   password
   * @return    a connection object specified by the connection string
   * @exception SQLException SQLException
   * @exception ClassNotFoundException ClassNotFoundException
   *
   */
  public static Connection connect(String sid, String host, 
				    String port, String driver, 
				    String username, String passwd)
    throws SQLException, ClassNotFoundException
  {  
    Class.forName ("oracle.jdbc.OracleDriver");
   
    Map<String, String> env = System.getenv();
    if(env.get("TWO_TASK")!=null && env.get("TWO_TASK").equals("cdb1_pdb1")) {
           return connectCDB(sid, host, port, driver, username, passwd);
    }

    // construct url from info given
    if (driver.toLowerCase().compareTo("oci7") == 0)
      {
        url = new String ("jdbc:oracle:oci7:");
        url = url.concat("@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(PORT=" + port);
        url = url.concat(")(HOST=" + host + "))(CONNECT_DATA=(SID=" + sid + ")))");
        //url = url.concat("@(DESCRIPTION=(ADDRESS=(PROTOCOL=ipc)(KEY=" + sid);
	//url = url.concat("))(CONNECT_DATA=(SID=" + sid + ")))");
      }
    else if (driver.toLowerCase().compareTo("oci8") == 0)
      {
        url = new String ("jdbc:oracle:oci8:");
        url = url.concat("@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(PORT=" + port);
        url = url.concat(")(HOST=" + host + "))(CONNECT_DATA=(SID=" + sid + ")))");
        //url = url.concat("@(DESCRIPTION=(ADDRESS=(PROTOCOL=ipc)(KEY=" + sid);
	//url = url.concat("))(CONNECT_DATA=(SID=" + sid + ")))");
      }
    else if (driver.toLowerCase().compareTo("thin") == 0)
      {
        url = new String ("jdbc:oracle:thin:");
        url = url.concat("@" + host + ":" + port + ":" + sid);
      }
    else
      {
         System.out.println ("unsupported driver");
      }


    ConnectDesc = "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(PORT=" + port +
                  ")(HOST=" + host + "))(CONNECT_DATA=(SID=" + sid + ")))";

    //ConnectDesc = "(DESCRIPTION=(ADDRESS=(PROTOCOL=ipc)(KEY=" + sid +
    //              "))(CONNECT_DATA=(SID=" + sid + ")))";

    ConnectIP = host;
    ConnectPort = port;
    ConnectSID = sid;

    ConnectID = username;
    ConnectPass = passwd;

    System.out.println("Connect as : " + username + ":" + passwd +
			" driver: " + driver);
    return DriverManager.getConnection(url, username, passwd);

  } 
  /**
   * Returns a Connection method.
   *
   * @param     sid      oracle sid
   * @param     host     database host
   * @param     port     listening port number
   * @param     driver   oci7 or oci8 or thin
   * @param     username username
   * @param     passwd   password
   * @return    a connection object specified by the connection string
   * @exception SQLException SQLException
   * @exception ClassNotFoundException ClassNotFoundException
   *
   */
  public static Connection connectCDB(String sid, String host,
                                    String port, String driver,
                                    String username, String passwd)
    throws SQLException, ClassNotFoundException
  {
    return connectCDB(sid, host, port, driver, username, passwd, 0);
  }

  /**
   * Returns a Connection method.
   *
   * @param     sid      oracle sid
   * @param     host     database host
   * @param     port     listening port number
   * @param     driver   oci7 or oci8 or thin
   * @param     username username
   * @param     passwd   password
   * @param     instid   specific instance to connect to (0 for any instacne)
   * @return    a connection object specified by the connection string
   * @exception SQLException SQLException
   * @exception ClassNotFoundException ClassNotFoundException
   *
   */
  public static Connection connectCDB(String sid, String host,
                                    String port, String driver,
                                    String username, String passwd, int instid)
    throws SQLException, ClassNotFoundException
  {
    Class.forName ("oracle.jdbc.OracleDriver");

    // construct url from info given
    if (driver.toLowerCase().compareTo("oci7") == 0)
      {
        url = new String ("jdbc:oracle:oci7:");
        url = url.concat("@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(PORT=" + port);
        url = url.concat(")(HOST=" + host + "))(CONNECT_DATA=(SERVICE_NAME=cdb1_pdb1" + ((instid ==0) ? "" : ("_i" + Integer.toString(instid))) + ".regress.rdbms.dev.us.oracle.com)))");
        //url = url.concat("@(DESCRIPTION=(ADDRESS=(PROTOCOL=ipc)(KEY=" + sid);
        //url = url.concat("))(CONNECT_DATA=(SID=" + sid + ")))");
      }
    else if (driver.toLowerCase().compareTo("oci8") == 0)
      {
        url = new String ("jdbc:oracle:oci8:");
        url = url.concat("@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(PORT=" + port);
        url = url.concat(")(HOST=" + host + "))(CONNECT_DATA=(SERVICE_NAME=cdb1_pdb1" + ((instid ==0) ? "" : ("_i" + Integer.toString(instid))) + ".regress.rdbms.dev.us.oracle.com)))");
        //url = url.concat("@(DESCRIPTION=(ADDRESS=(PROTOCOL=ipc)(KEY=" + sid);
        //url = url.concat("))(CONNECT_DATA=(SID=" + sid + ")))");
      }
    else if (driver.toLowerCase().compareTo("thin") == 0)
      {
        url = new String ("jdbc:oracle:thin:");
        url = url.concat("@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(PORT=" + port);
        url = url.concat(")(HOST=" + host + "))(CONNECT_DATA=(SERVICE_NAME=cdb1_pdb1" + ((instid ==0) ? "" : ("_i" + Integer.toString(instid))) + ".regress.rdbms.dev.us.oracle.com)))");
      }
    else
      {
         System.out.println ("unsupported driver");
      }


    ConnectDesc = "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(PORT=" + port +
                  ")(HOST=" + host + "))(CONNECT_DATA=(SERVICE_NAME=cdb1_pdb1" + ((instid ==0) ? "" : ("_i" + Integer.toString(instid))) + ".regress.rdbms.dev.us.oracle.com)))";

    //ConnectDesc = "(DESCRIPTION=(ADDRESS=(PROTOCOL=ipc)(KEY=" + sid +
    //              "))(CONNECT_DATA=(SID=" + sid + ")))";

    ConnectIP = host;
    ConnectPort = port;
    ConnectSID = sid;

    ConnectID = username;
    ConnectPass = passwd;

    System.out.println("Connect as : " + username + ":" + passwd +
                        " driver: " + driver);
    return DriverManager.getConnection(url, username, passwd);

  }

  public static Connection connectSys(String args[])
    throws SQLException, ClassNotFoundException
  {  
    Properties   info     = new Properties();
    Connection   sys_conn = null;
    String       conn_url = null;

    Class.forName ("oracle.jdbc.OracleDriver");

    /* FIXME: for sys always use driver oci8 - others don't seem to work */

    conn_url = new String ("jdbc:oracle:oci8:@");

    /* create generic URL */
    createURL(args);

    /* updated from info.put("internal_logon", "true"); for Oracle 9 */
    info.put("user","sys");
    info.put("password","knl_test7");
    info.put("internal_logon","sysdba");
 
    try {
      sys_conn = DriverManager.getConnection(conn_url, info);
    }
    catch (SQLException e)
    { // try another password
      info.put("password","change_on_install");
      sys_conn = DriverManager.getConnection(conn_url, info);
    }

    return sys_conn;
  }

  public static String createURL (String args[])
    throws SQLException
  {
    
    if(args == null)
      return null;

    return createURL(args[0], args[1], args[2], args[3]);

  }

  public static String createURL (String sid, String host, 
				  String port, String driver)
    throws SQLException
  {  
    return createURL(sid, host, port, driver, null);
  }

  public static String createURL (String sid, String host, 
				  String port, String driver, int instid)
    throws SQLException
  {  
    return createURL(sid, host, port, driver, null);
  }

  /**
   * Returns a Connection URL
   *
   * @param     sid      oracle sid
   * @param     host     database host
   * @param     port     listening port number
   * @param     driver   oci7 or oci8 or thin
   * @param     service  ORACLE_SID or null
   * @return    a connection URL
   * @exception SQLException SQLException
   * @exception ClassNotFoundException ClassNotFoundException
   *
   */
  public static String createURL (String sid, String host, 
				  String port, String driver, String service)
    throws SQLException
  {  
   
     Map<String, String> env = System.getenv();
     if(env.get("TWO_TASK")!=null && env.get("TWO_TASK").equals("cdb1_pdb1")) {
           return createCDBURL(sid, host, port, driver, 0);
     }


    // construct url from info given
    if (driver == null ) {
      url = new String ("jdbc:oracle:null:");
      url = url.concat("@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(PORT=" + port);
      url = url.concat(")(HOST=" + host + "))(CONNECT_DATA=(SID=" + sid + ")))");
    }
    else if (driver.toLowerCase().compareTo("kprb") == 0) {
      url = new String ("jdbc:oracle:kprb:");
      url = url.concat("@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(PORT=" + port);
      url = url.concat(")(HOST=" + host + "))(CONNECT_DATA=(SID=" + sid + ")))");
    }
    else if (driver.toLowerCase().compareTo("oci7") == 0)
    {
      url = new String ("jdbc:oracle:oci7:");
      url = url.concat("@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(PORT=" + port);
      url = url.concat(")(HOST=" + host + "))(CONNECT_DATA=(SID=" + sid + ")))");
      //url = url.concat("@(DESCRIPTION=(ADDRESS=(PROTOCOL=ipc)(KEY=" + sid);
      //url = url.concat("))(CONNECT_DATA=(SID=" + sid + ")))");
    }
    else if (driver.toLowerCase().compareTo("oci8") == 0 || driver.toLowerCase().compareTo("oci") == 0)
    {
      url = new String ("jdbc:oracle:"+driver.toLowerCase()+":");
      url = url.concat("@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(PORT=" + port);
      url = url.concat(")(HOST=" + host + "))(CONNECT_DATA=(SID=" + sid + ")))");
      //url = url.concat("@(DESCRIPTION=(ADDRESS=(PROTOCOL=ipc)(KEY=" + sid);
      //url = url.concat("))(CONNECT_DATA=(SID=" + sid + ")))");
    }
    else if (driver.toLowerCase().compareTo("thin") == 0)
    {
      if (service == null)
      {
      url = new String ("jdbc:oracle:thin:");
      url = url.concat("@" + host + ":" + port + ":" + sid);
      }
      else
      {
      String service_name = service + ".regress.rdbms.dev.us.oracle.com";
      url = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(host=" +
            host + ")(port=" + port + "))(CONNECT_DATA=(INSTANCE_NAME=" + sid +
            ")" + "(SERVICE_NAME=" + service_name + ")))";
      }
    }
    else
    {
      System.out.println ("unsupported driver");
    }

    ConnectDesc = "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(PORT=" + port +
                  ")(HOST=" + host + "))(CONNECT_DATA=(SID=" + sid + ")))";

    //ConnectDesc = "(DESCRIPTION=(ADDRESS=(PROTOCOL=ipc)(KEY=" + sid +
    //              "))(CONNECT_DATA=(SID=" + sid + ")))";

    ConnectIP = host;
    ConnectPort = port;
    ConnectSID = sid;

    return url;
  }

 /**
   * Returns a Connection URL
   *
   * @param     sid      oracle sid
   * @param     host     database host
   * @param     port     listening port number
   * @param     driver   oci7 or oci8 or thin
   * @param     service  ORACLE_SID or null
   * @return    a connection URL
   * @exception SQLException SQLException
   * @exception ClassNotFoundException ClassNotFoundException
   *
   */
  public String createUrl (String sid, String host, 
				  String port, String driver, String service)
    throws SQLException
  {  
     String uRL = null;
     Map<String, String> env = System.getenv();
     if(env.get("TWO_TASK")!=null && env.get("TWO_TASK").equals("cdb1_pdb1")) {
           return createCDBUrl(sid, host, port, driver, 0);
     }


    // construct url from info given
    if (driver == null ) {
    	uRL = new String ("jdbc:oracle:null:");
    	uRL = uRL.concat("@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(PORT=" + port);
    	uRL = uRL.concat(")(HOST=" + host + "))(CONNECT_DATA=(SID=" + sid + ")))");
    }
    else if (driver.toLowerCase().compareTo("kprb") == 0) {
    	uRL = new String ("jdbc:oracle:kprb:");
    	uRL = uRL.concat("@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(PORT=" + port);
    	uRL = uRL.concat(")(HOST=" + host + "))(CONNECT_DATA=(SID=" + sid + ")))");
    }
    else if (driver.toLowerCase().compareTo("oci7") == 0)
    {
    	uRL = new String ("jdbc:oracle:oci7:");
    	uRL = uRL.concat("@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(PORT=" + port);
    	uRL = uRL.concat(")(HOST=" + host + "))(CONNECT_DATA=(SID=" + sid + ")))");
      //url = url.concat("@(DESCRIPTION=(ADDRESS=(PROTOCOL=ipc)(KEY=" + sid);
      //url = url.concat("))(CONNECT_DATA=(SID=" + sid + ")))");
    }
    else if (driver.toLowerCase().compareTo("oci8") == 0 || driver.toLowerCase().compareTo("oci") == 0)
    {
    	uRL = new String ("jdbc:oracle:"+driver.toLowerCase()+":");
    	uRL = uRL.concat("@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(PORT=" + port);
    	uRL = uRL.concat(")(HOST=" + host + "))(CONNECT_DATA=(SID=" + sid + ")))");
      //url = url.concat("@(DESCRIPTION=(ADDRESS=(PROTOCOL=ipc)(KEY=" + sid);
      //url = url.concat("))(CONNECT_DATA=(SID=" + sid + ")))");
    }
    else if (driver.toLowerCase().compareTo("thin") == 0)
    {
      if (service == null)
      {
    	  uRL = new String ("jdbc:oracle:thin:");
          uRL = uRL.concat("@" + host + ":" + port + ":" + sid);
      }
      else
      {
      String service_name = service + ".regress.rdbms.dev.us.oracle.com";
      uRL = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(host=" +
            host + ")(port=" + port + "))(CONNECT_DATA=(INSTANCE_NAME=" + sid +
            ")" + "(SERVICE_NAME=" + service_name + ")))";
      }
    }
    else
    {
      System.out.println ("unsupported driver");
    }

    /*ConnectDesc = "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(PORT=" + port +
                  ")(HOST=" + host + "))(CONNECT_DATA=(SID=" + sid + ")))";

    //ConnectDesc = "(DESCRIPTION=(ADDRESS=(PROTOCOL=ipc)(KEY=" + sid +
    //              "))(CONNECT_DATA=(SID=" + sid + ")))";

    ConnectIP = host;
    ConnectPort = port;
    ConnectSID = sid;*/

    return uRL;
  }
 
  /**
   * Returns a Connection URL
   *
   * @param     sid      oracle sid
   * @param     host     database host
   * @param     port     listening port number
   * @param     driver   oci7 or oci8 or thin
   * @param     instid   specific instance to connect to (0 for any instacne)
   * @return    a connection URL
   * @exception SQLException SQLException
   * @exception ClassNotFoundException ClassNotFoundException
   *
   */
  public static String createCDBURL (String sid, String host,
                                  String port, String driver, int instid)
    throws SQLException
  {
    // construct url from info given
    if (driver == null ) {
      url = new String ("jdbc:oracle:null:");
      url = url.concat("@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(PORT=" + port);
      url = url.concat(")(HOST=" + host + "))(CONNECT_DATA=(SERVICE_NAME=cdb1_pdb1" + ((instid ==0) ? "" : ("_i" + 
                   Integer.toString(instid))) + ".regress.rdbms.dev.us.oracle.com)))");

    }
    else if (driver.toLowerCase().compareTo("kprb") == 0) {
      url = new String ("jdbc:oracle:kprb:");
      url = url.concat("@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(PORT=" + port);
      url = url.concat(")(HOST=" + host + "))(CONNECT_DATA=(SERVICE_NAME=cdb1_pdb1" + ((instid ==0) ? "" : ("_i" + 
                   Integer.toString(instid))) + ".regress.rdbms.dev.us.oracle.com)))");

    }

    else if (driver.toLowerCase().compareTo("oci7") == 0)
    {
      url = new String ("jdbc:oracle:oci7:");
      url = url.concat("@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(PORT=" + port);
      url = url.concat(")(HOST=" + host + "))(CONNECT_DATA=(SERVICE_NAME=cdb1_pdb1" + ((instid ==0) ? "" : ("_i" + 
                   Integer.toString(instid))) + ".regress.rdbms.dev.us.oracle.com)))");
      //url = url.concat("@(DESCRIPTION=(ADDRESS=(PROTOCOL=ipc)(KEY=" + sid);
      //url = url.concat("))(CONNECT_DATA=(SID=" + sid + ")))");
    }
    else if (driver.toLowerCase().compareTo("oci8") == 0 || driver.toLowerCase().compareTo("oci") == 0)
    {
      url = new String ("jdbc:oracle:"+driver.toLowerCase()+":");
      url = url.concat("@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(PORT=" + port);
      url = url.concat(")(HOST=" + host + "))(CONNECT_DATA=(SERVICE_NAME=cdb1_pdb1" + ((instid ==0) ? "" : ("_i" + 
                   Integer.toString(instid))) + ".regress.rdbms.dev.us.oracle.com)))");
      //url = url.concat("@(DESCRIPTION=(ADDRESS=(PROTOCOL=ipc)(KEY=" + sid);
      //url = url.concat("))(CONNECT_DATA=(SID=" + sid + ")))");
    }
    else if (driver.toLowerCase().compareTo("thin") == 0)
    {
      url = new String ("jdbc:oracle:thin:");
      url = url.concat("@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(PORT=" + port);
      url = url.concat(")(HOST=" + host + "))(CONNECT_DATA=(SERVICE_NAME=cdb1_pdb1" + ((instid ==0) ? "" : ("_i" + 
                   Integer.toString(instid))) + ".regress.rdbms.dev.us.oracle.com)))");
    }
    else
    {
      System.out.println ("unsupported driver");
    }

    ConnectDesc = "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(PORT=" + port +
                  ")(HOST=" + host + "))(CONNECT_DATA=(SERVICE_NAME=cdb1_pdb1" + ((instid ==0) ? "" : ("_i" + 
                  Integer.toString(instid))) + ".regress.rdbms.dev.us.oracle.com)))";

    //ConnectDesc = "(DESCRIPTION=(ADDRESS=(PROTOCOL=ipc)(KEY=" + sid +
    //              "))(CONNECT_DATA=(SID=" + sid + ")))";

    ConnectIP = host;
    ConnectPort = port;
    ConnectSID = sid;

    return url;
   }

   /**
   * Returns a Connection URL
   *
   * @param     sid      oracle sid
   * @param     host     database host
   * @param     port     listening port number
   * @param     driver   oci7 or oci8 or thin
   * @param     instid   specific instance to connect to (0 for any instacne)
   * @return    a connection URL
   * @exception SQLException SQLException
   * @exception ClassNotFoundException ClassNotFoundException
   *
   */
  public String createCDBUrl (String sid, String host,
                                  String port, String driver, int instid)
    throws SQLException
  {
    String uRL = null;
	 // construct url from info given
    if (driver == null ) {
    	uRL = new String ("jdbc:oracle:null:");
    	uRL = uRL.concat("@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(PORT=" + port);
    	uRL = uRL.concat(")(HOST=" + host + "))(CONNECT_DATA=(SERVICE_NAME=cdb1_pdb1" + ((instid ==0) ? "" : ("_i" + 
                   Integer.toString(instid))) + ".regress.rdbms.dev.us.oracle.com)))");

    }
    else if (driver.toLowerCase().compareTo("kprb") == 0) {
    	uRL = new String ("jdbc:oracle:kprb:");
    	uRL = uRL.concat("@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(PORT=" + port);
    	uRL = uRL.concat(")(HOST=" + host + "))(CONNECT_DATA=(SERVICE_NAME=cdb1_pdb1" + ((instid ==0) ? "" : ("_i" + 
                   Integer.toString(instid))) + ".regress.rdbms.dev.us.oracle.com)))");

    }

    else if (driver.toLowerCase().compareTo("oci7") == 0)
    {
    	uRL = new String ("jdbc:oracle:oci7:");
    	uRL = uRL.concat("@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(PORT=" + port);
    	uRL = uRL.concat(")(HOST=" + host + "))(CONNECT_DATA=(SERVICE_NAME=cdb1_pdb1" + ((instid ==0) ? "" : ("_i" + 
                   Integer.toString(instid))) + ".regress.rdbms.dev.us.oracle.com)))");
      //url = url.concat("@(DESCRIPTION=(ADDRESS=(PROTOCOL=ipc)(KEY=" + sid);
      //url = url.concat("))(CONNECT_DATA=(SID=" + sid + ")))");
    }
    else if (driver.toLowerCase().compareTo("oci8") == 0 || driver.toLowerCase().compareTo("oci") == 0)
    {
    	uRL = new String ("jdbc:oracle:"+driver.toLowerCase()+":");
    	uRL = uRL.concat("@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(PORT=" + port);
    	uRL = uRL.concat(")(HOST=" + host + "))(CONNECT_DATA=(SERVICE_NAME=cdb1_pdb1" + ((instid ==0) ? "" : ("_i" + 
                   Integer.toString(instid))) + ".regress.rdbms.dev.us.oracle.com)))");
      //url = url.concat("@(DESCRIPTION=(ADDRESS=(PROTOCOL=ipc)(KEY=" + sid);
      //url = url.concat("))(CONNECT_DATA=(SID=" + sid + ")))");
    }
    else if (driver.toLowerCase().compareTo("thin") == 0)
    {
    	uRL = new String ("jdbc:oracle:thin:");
    	uRL = uRL.concat("@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(PORT=" + port);
    	uRL = uRL.concat(")(HOST=" + host + "))(CONNECT_DATA=(SERVICE_NAME=cdb1_pdb1" + ((instid ==0) ? "" : ("_i" + 
                   Integer.toString(instid))) + ".regress.rdbms.dev.us.oracle.com)))");
    }
    else
    {
      System.out.println ("unsupported driver");
    }

    /*ConnectDesc = "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(PORT=" + port +
                  ")(HOST=" + host + "))(CONNECT_DATA=(SERVICE_NAME=cdb1_pdb1" + ((instid ==0) ? "" : ("_i" + 
                  Integer.toString(instid))) + ".regress.rdbms.dev.us.oracle.com)))";

    //ConnectDesc = "(DESCRIPTION=(ADDRESS=(PROTOCOL=ipc)(KEY=" + sid +
    //              "))(CONNECT_DATA=(SID=" + sid + ")))";

    ConnectIP = host;
    ConnectPort = port;
    ConnectSID = sid;*/

    return uRL;
   }

  /**
   * Returns a Connection URL
   *
   * @param     sid      oracle sid
   * @param     host     database host
   * @param     port     listening port number
   * @param     driver   oci7 or oci8 or thin
   * @return    QueueConnectionFactory
   * @exception SQLException SQLException
   * @exception JMSException JMSException
   *
   */
  public static QueueConnectionFactory getQueueConnectionFactory(String sid, String host,
                String port,String driver) throws SQLException, JMSException
  {
	return getQueueConnectionFactory(sid, host, port, driver, null, null);
  }
  /**
   * Returns a Connection URL
   *
   * @param     sid      oracle sid
   * @param     host     database host
   * @param     port     listening port number
   * @param     driver   oci7 or oci8 or thin
   * @return    TopicConnectionFactory
   * @exception SQLException SQLException
   * @exception JMSException JMSException
   *
   */
  public static TopicConnectionFactory getTopicConnectionFactory(String sid, String host,
                String port,String driver) throws SQLException, JMSException
  {
	return getTopicConnectionFactory(sid, host, port, driver, null, null);
  }
  /**
   * Returns a Connection URL
   *
   * @param     sid      oracle sid
   * @param     host     database host
   * @param     port     listening port number
   * @param     driver   oci7 or oci8 or thin
   * @return    QueueConnectionFactory
   * @exception SQLException SQLException
   * @exception JMSException JMSException
   *
   */
  public static QueueConnectionFactory getQueueConnectionFactory(String sid, String host, 
		String port,String driver, String user, String password) throws SQLException, JMSException 
  {

      String url = createURL(sid, host, ""+port, driver);
      Properties myProperties = new Properties();
      if(user != null) 
	      myProperties.put("user", user);
      if(password != null) 
	      myProperties.put("password", password);
      return AQjmsFactory.getQueueConnectionFactory(url, myProperties);

  }
   /**
   * Returns a Connection URL
   *
   * @param     sid      oracle sid
   * @param     host     database host
   * @param     port     listening port number
   * @param     driver   oci7 or oci8 or thin
   * @return    TopicConnectionFactory
   * @exception SQLException SQLException
   * @exception JMSException JMSException
   *
   */
  public static TopicConnectionFactory getTopicConnectionFactory(String sid, String host,
                String port,String driver, String user, String password) throws SQLException, JMSException
  {

      String url = createURL(sid, host, ""+port, driver);
      Properties myProperties = new Properties();
      if(user != null) 
	      myProperties.put("user", user);
      if(password != null)
	      myProperties.put("password", password);
      return AQjmsFactory.getTopicConnectionFactory(url, myProperties);

  }

  /**
   * Returns the URL as a Java String Object
   *
   * @return    the URL as a Java String Object
   *
   */
  static String getURL() 
  {
    return (url);
  }
 
  /**
   * Returns the USER as a Java String Object
   *
   * @return    the USER as a Java String Object
   *
   */
  static String getID()
  {
    return (ConnectID);
  }
  
  /**
   * Returns the PASSWORD as a Java String Object
   *
   * @return    the PASSWORD as a Java String Object
   *
   */
  static String getPass()
  {
    return (ConnectPass);
  }

  /**
   * Returns the Oracle Connection Description as a Java String Object
   *
   * @return    the Oracle Connection Description as a Java String Object
   *
   */
  static String getConnectDesc() 
  {
    return (ConnectDesc);
  }

  /**
   * Returns the IP ADDRESS as a Java String Object
   *
   * @return    the IP ADDRESS as a Java String Object
   *
   */
  static String getConnectIP() 
  {
    return (ConnectIP);
  }

  /**
   * Returns the PORT NUMBER as a Java String Object
   *
   * @return    the PORT NUMBER as a Java String Object
   *
   */
  static String getConnectPort() 
  {
    return (ConnectPort);
  }

  /**
   * Returns the ORACLE SID as a Java String Object
   *
   * @return    the ORACLE SID as a Java String Object
   *
   */ 
  static String getConnectSID() 
  {
    return (ConnectSID);
  }

  /**
   * Create a test user on the database
   *
   * @param     url      JDBC url
   * @param     userID   user id
   * @param     userPass user password
   * @exception SQLException SQLException
   * @exception ClassNotFoundException ClassNotFoundException
   *
   */
  public static void createUser(Connection db_conn, String url,
			 String userID, String userPass,
			 boolean admin)
    throws SQLException, ClassNotFoundException
  {
    createUser(db_conn, url, userID, userPass, admin, false);
  }

  public static void createUser(Connection db_conn, String url,
			 String userID, String userPass,
			 boolean admin, boolean is_dbjava)
    throws SQLException, ClassNotFoundException
  {
    Connection  conn = null;

    ConnectID   = userID;
    ConnectPass = userPass;

    if(db_conn != null)
    {
      conn = db_conn;
    }
    else
    {
      Class.forName ("oracle.jdbc.OracleDriver");
      conn = DriverManager.getConnection(url, "sys as sysdba", "knl_test7");
    }
    
    Statement stmt = conn.createStatement();

    try 
    {
      stmt.execute("drop user " + ConnectID + " cascade");
    } 
    catch (Exception ex) 
    {}

    if(admin)
    {
      stmt.execute("create user " + ConnectID + " identified by " + ConnectPass);
      stmt.execute("grant connect, resource, unlimited tablespace, aq_administrator_role to " + ConnectID);

      stmt.execute("grant execute on sys.dbms_aqadm to " + ConnectID);
      stmt.execute("grant execute on sys.dbms_aq to " + ConnectID);

      if (is_dbjava)
      {
        stmt.execute("{call dbms_java.grant_permission( '"
            +ConnectID.toUpperCase() 
            +"', 'SYS:java.lang.RuntimePermission', 'getClassLoader', '' )}");
        stmt.execute("{call dbms_java.grant_permission( '"
            +ConnectID.toUpperCase()
            +"', 'SYS:java.lang.RuntimePermission', "
            +"'setContextClassLoader', '' )}");
      }
    }
    else
    {
      stmt.execute("create user " + ConnectID + " identified by " + ConnectPass);
      stmt.execute("grant connect, resource, unlimited tablespace, aq_user_role to " + ConnectID);

      stmt.execute("grant execute on sys.dbms_aq to " + ConnectID);
    }

    /* FIX ME: Only for testing - remove this ASAP */
    //stmt.execute("grant execute on sys.dbms_aqin to " + ConnectID);

    stmt.close();

    if(db_conn == null)
      conn.close();
  }

 /**
   * Display the content of a resultSet object
   *
   * @param     rset     an Oracle ResultSet
   * @exception SQLException SQLException
   *
   */
 static void showResultSet (ResultSet rset)
    throws SQLException
  {
    ResultSetMetaData md = rset.getMetaData ();
    int columns = md.getColumnCount ();
    int i;
   
    System.out.println();
    System.out.println("Printing Result from test");

    for (i = 0; i < columns; i++)
    {
      System.out.print (md.getColumnName (i + 1) + ": ");
      System.out.print ("type " + md.getColumnType (i + 1) + ", ");
      System.out.print ("type name " + md.getColumnTypeName (i + 1));
      // System.out.print ("size " + md.getColumnDisplaySize (i + 1) + ", ");
      // System.out.print ("precision " + md.getPrecision (i + 1) + ", ");
      // System.out.print ("scale " + md.getScale (i + 1) + ", ");
      // System.out.print ("signed " + md.isSigned (i + 1) + ", ");
      System.out.println ();
    }

    // for (i = 0; i < columns; i++)
    // {
    //  System.out.print (md.getColumnName (i + 1));
    //  System.out.print ("\t");
    // }

    System.out.println ();
    while (rset.next ())
    {
      for (i = 0; i < columns; i++)
      {
         System.out.print (md.getColumnName (i + 1) + ": " );
        switch (md.getColumnType (i + 1))
        {
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
          {
            String s = rset.getString (i + 1);
            System.out.print (s);
          }
          break;

        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
          printByteArray (rset.getBytes (i + 1));
          break;

        case Types.DATE:
        case Types.TIME:
        case Types.TIMESTAMP:
          System.out.print (rset.getDate (i + 1));
          break;

        case Types.NUMERIC:
        case Types.DECIMAL:
          System.out.print (rset.getString (i + 1));
          break;

        case oracle.jdbc.OracleTypes.BLOB:
          printByteArray (rset.getBytes (i + 1));
          break;

        default:
          System.out.print (rset.getString (i + 1) + " (as "
                            + md.getColumnTypeName (i + 1) + ")");
        }
        System.out.println ();
      }

      System.out.println ();
    }

    System.out.println ();
  }

  /**
   * Print the content of the byte array in Hex
   *
   * @param     b    byte array
   *
   */
  static void printByteArray (byte [] b)
  {
    int i;
    if (b == null)
      System.out.println ("<Null byte array!>");
    else
    {
      int n = b.length;
      for (i = 0; i < n; i++)
      {
	int v = b [i] & 0xff;
	if (v < 0x10)
	  System.out.print ("0" + Integer.toHexString (v) + " ");
	else
	  System.out.print (Integer.toHexString (v) + " ");
      }
      System.out.println ();
    }
  }

  /**
   * Try to execute the sql statement.  Doesn't return
   * the status of the execution.
   *
   * @param     connection   an open Oracle connection
   * @param     sql  the sql statement to execute
   * @exception SQLException SQLException
   *
   */
  public static void trySQL (Connection connection, String sql)
    throws SQLException
  {
    Statement stmt = connection.createStatement ();
    try
    {
      stmt.execute (sql);
    }
    catch (SQLException e)
    {
    }
    finally
    {
      stmt.close ();
    }
  }

  /**
   * Try to execute the sql statement.  Throws exception
   * if statement fails.
   *
   * @param     connection   an open Oracle connection
   * @param     sql  the sql statement to execute
   * @exception SQLException SQLException
   *
   */
  public static void doSQL (Connection connection, String sql)
    throws SQLException
  {
    Statement stmt = null;
    try
    {
      stmt = connection.createStatement ();
      stmt.execute (sql);
    } 
    finally
    {
      if (stmt != null)
        stmt.close();
    }
  }

  /**
   * Print a character array in hex.
   *
   * @param     b character array
   *
   */
  static void printCharArray (char [] b)
  {
    int i;
    if (b == null)
      System.out.println ("<Null char array!>");
    else
    {
      int n = b.length;
      for (i = 0; i < n; i++)
        System.out.print ("0x" + Integer.toHexString (b [i] & 0xffff) + " ");
      System.out.println ();
    }
  }

  /**
   * Print a character array in Unicode format
   *
   * @param     b character array
   *
   */
  static void printCharArrayUnicode (char [] b)
  {
    int i;
    if (b == null)
      System.out.println ("<Null char array!>");
    else
    {
      int n = b.length;
      for (i = 0; i < n; i++)
        System.out.print ("\\u" + Integer.toHexString (b [i] & 0xffff));
    }
  }

  /**
   * Print the content of an InputStream as integer
   *
   * @param     s InputStream
   *
   */
  static void dump_stream (InputStream s)
  {
    try
    {
      int c;
      while ((c = s.read ()) != -1)
        System.out.print (c + " ");
      System.out.println ();
    }
    catch (IOException e)
    {
      e.printStackTrace ();
    }
  }

  /**
   * Print the content of an InputStream in Hex
   *
   * @param     s InputStream
   *
   */
  static void hexDumpStream (InputStream s) 
  { 
    try 
    { 
      int c; 
      while ((c = s.read ()) != -1) 
      {
	c = c & 0xff;
	if (c < 0x10)
	  System.out.print ("0" + Integer.toHexString (c) + " "); 
	else
	  System.out.print (Integer.toHexString (c & 0xff) + " "); 
      }
      System.out.println (); 
    } 
    catch (IOException e) 
    { 
      e.printStackTrace (); 
    } 
  }


  public static void dropUser(Connection db_conn, String UserName) 
                      throws SQLException, ClassNotFoundException
  {
    Connection conn = null;

    if(db_conn != null)
      conn = db_conn;
    else
      conn = connectSys(null);
    
    Statement stmt = conn.createStatement();

    try 
    {
      stmt.execute("drop user " + UserName + " cascade");

    } 
    catch (Exception ex) 
    {
      //System.out.println("Drop user failed : " + ex);
    }

    stmt.close();

    if(db_conn == null)
      conn.close();
  }


//==========================================================================
  public static void selectUserQTables(Connection db_conn) throws SQLException
  {
    CallableStatement    sel_stmt = null;

    /* Select from user_queue_tables to see if table got created */
    sel_stmt = db_conn.prepareCall ("select  queue_table, type, user_comment, object_type, sort_order from user_queue_tables order by queue_table");

    ResultSet rset = sel_stmt.executeQuery ();

    System.out.println ("\n[ User_queue_tables ]");
    while (rset.next ())
    {
      // System.out.print (rset.getString (1));
      // System.out.print ("\t  " + rset.getString (2));
      // System.out.print ("\t  " + rset.getString (3));
      // System.out.print ("\t  " + rset.getString (4));
      // System.out.println ("\t  " + rset.getString (5));
      System.out.print(rpadString(rset.getString(1), 30));
      System.out.print(rpadString(rset.getString(2), 7));
      System.out.print(rpadString(rset.getString(3), 30));
      System.out.print(rpadString(rset.getString(4), 30));
      System.out.println (rset.getString (5));
    }
    System.out.println ("");
    sel_stmt.close();
  }


  //==========================================================================
  public static void selectUserQueues(Connection db_conn, int level) throws SQLException
{
   selectUserQueues(db_conn, level, true);
}

  //
  // set usePadding to false for problems with NT difs.  oratst run on NT
  // is splitting long output lines with carriage returns.
  //
  public static void selectUserQueues(Connection db_conn, int level, boolean usePadding) throws SQLException
  {
    CallableStatement    sel_stmt = null;
    ResultSet            rset     = null;

    /* Select from user_queue to see if queues got created */
    sel_stmt = db_conn.prepareCall ("select  name, queue_table, queue_type, enqueue_enabled, dequeue_enabled, user_comment, max_retries, retry_delay, retention from user_queues order by name");

    rset = sel_stmt.executeQuery ();
      
    System.out.println ("\n[ User_queues ]");
    while (rset.next ())
    {
     // the old way
      if (usePadding)    // the old way
      {
        // System.out.print (rset.getString (1));
        // System.out.print ("\t  " + rset.getString (2));
        // System.out.print ("\t  " + rset.getString (3));
        System.out.print(rpadString(rset.getString(1), 30));
        System.out.print(rpadString(rset.getString(2), 30));
        System.out.print(rpadString(rset.getString(3), 20));
        System.out.print ("  " + rset.getString (4));
        System.out.print ("  " + rset.getString (5));
        System.out.print ("  " + rset.getString (6));
      
        if(level > 0)
        {
	  System.out.print ("  " + rset.getString (7));
	  System.out.print ("  " + rset.getString (8));
	  System.out.print("  " + rset.getString (9));
        }
      }
      else  // generally for NT, no long output lines
      {
        System.out.println(rset.getString(1));
        System.out.println(rset.getString(2));
        System.out.println(rset.getString(3));
        System.out.println(rset.getString(4));
        System.out.println(rset.getString(5));
        System.out.println(rset.getString(6));
      
        if(level > 0)
        {
	  System.out.println(rset.getString (7));
	  System.out.println(rset.getString (8));
	  System.out.println(rset.getString (9));
        }
        System.out.print("\n");
      }

      System.out.print("\n\n");
    }
    sel_stmt.close();
  }

  //==========================================================================
  public static void selectUserQueue(Connection db_conn, String queue_name,
				     int level) throws SQLException
  {
    CallableStatement    sel_stmt = null;
    ResultSet            rset     = null;

    /* Select from user_queue to see if queues got created */
    sel_stmt = db_conn.prepareCall ("select  name, queue_table, queue_type, enqueue_enabled, dequeue_enabled, user_comment, max_retries, retry_delay, retention from user_queues where name = ?");

    sel_stmt.setString(1, queue_name.toUpperCase());

    rset = sel_stmt.executeQuery ();
      
    System.out.println ("\n[ User_queue - " + queue_name + " ]");
    while (rset.next ())
    {
      // System.out.print (rset.getString (1));
      // System.out.print ("\t  " + rset.getString (2));
      // System.out.print ("\t  " + rset.getString (3));
      System.out.print(rpadString(rset.getString(1), 30));
      System.out.print(rpadString(rset.getString(2), 30));
      System.out.print(rpadString(rset.getString(3), 20));
      System.out.print ("  " + rset.getString (4));
      System.out.print ("  " + rset.getString (5));
      System.out.print ("  " + rset.getString (6));
      
      if(level > 0)
      {
	System.out.print ("  " + rset.getString (7));
	System.out.print ("  " + rset.getString (8));
	System.out.print("  " + rset.getString (9));
      }

      System.out.print("\n\n");
    }
    sel_stmt.close();
  }


  //==========================================================================
  // This is the original (no sort) version of selectUserMessages
  //
  public static void selectUserMessages(Connection db_conn, String queue_table)
                                       throws SQLException
  {
    selectUserMessages(db_conn, queue_table, false, (String) null);
  }

  //==========================================================================
  // Use this if you want the sort option but not nothing for where clause
  //
  public static void selectUserMessages(Connection db_conn, String queue_table,
                                        boolean sortit)
                                       throws SQLException
  {
    selectUserMessages(db_conn, queue_table, sortit, (String) null);
  }

  //==========================================================================
  // Use this to add a where statement to query "where <whereClause>"
  //
  public static void selectUserMessages(Connection db_conn, String queue_table,
                                        boolean sortit, String whereClause)
                                       throws SQLException
  {
    CallableStatement    sel_stmt = null;
    String               owner    = null;
    String               table    = null;
    int                  idx      = 0;
    String               stmt     = null;
    String               stmt0    = null;

    idx = queue_table.indexOf(".");

    if(idx != -1)
    {
      owner = queue_table.substring(0, idx);
      table = queue_table.substring(idx + 1);

      stmt0 = "select queue, msg_id, corr_id from " + owner.toUpperCase() + ".AQ$" + table.toUpperCase();
    }
    else
    {
      stmt0 = "select queue, msg_id, corr_id from " + "AQ$" + queue_table.toUpperCase();
    }

    if (whereClause != null)
    {
      stmt0 = stmt0 + " where "+whereClause;
    }

    if (sortit)
    {
      stmt = stmt0 + " order by queue, corr_id";
    }
    else
    {
      stmt = stmt0;
    } 

    /* Select from user_queue_tables to see if there are any messages */
    sel_stmt = db_conn.prepareCall (stmt);

    ResultSet rset = sel_stmt.executeQuery ();

    System.out.println ("\n[ User_messages ]");
    while (rset.next ())
    {
      System.out.print (rpadString(rset.getString (1), 30));
      // System.out.print("\t  " + rset.getString (2));
      // System.out.println ("\t  " + rset.getString (3));
      System.out.print("    " + rset.getString (2));
      System.out.println ("    " + rset.getString (3));
    }

    System.out.print("\n");
    sel_stmt.close();
  }

  //==========================================================================
  public static void selectQueuePrivileges(Connection db_conn)
                                       throws SQLException
  {
    selectQueuePrivileges(db_conn, null);
  }

  //==========================================================================
  public static void selectQueuePrivileges(Connection db_conn, 
                                           String whereClause)
                                       throws SQLException
  {
    CallableStatement    sel_stmt = null;
    String               owner    = null;
    String               table    = null;
    int                  idx      = 0;
    String               stmt     = null;

    if (whereClause != null)
      stmt = "select owner, name, grantor, grantee, enqueue_privilege, dequeue_privilege from queue_privileges "+whereClause+" order by owner, name";
    else
      stmt = "select owner, name, grantor, grantee, enqueue_privilege, dequeue_privilege from queue_privileges order by owner, name";
          
    /* Select from queue_privileges to see what privileges we have */
    sel_stmt = db_conn.prepareCall (stmt);

    ResultSet rset = sel_stmt.executeQuery ();

    System.out.println ("\n[ Queue_privileges ]");
    while (rset.next ())
    {
      // System.out.print (rset.getString (1));
      // System.out.print("\t  " + rset.getString (2));
      // System.out.print("\t  " + rset.getString (3));
      // System.out.print("\t  " + rset.getString (4));
      // System.out.print("\t  " + rset.getString (5));
      // System.out.println ("\t  " + rset.getString (6));
      System.out.print (rpadString(rset.getString (1), 30));
      System.out.print (rpadString(rset.getString (2), 30));
      System.out.print (rpadString(rset.getString (3), 30));
      System.out.print (rpadString(rset.getString (4), 30));
      System.out.print("  " + rset.getString (5));
      System.out.println ("    " + rset.getString (6));
    }

    System.out.print("\n");
    sel_stmt.close();
  }

  //==========================================================================
  public static void selectUserRolePrivs(Connection db_conn)
                                       throws SQLException
  {
    CallableStatement    sel_stmt = null;
    String               owner    = null;
    String               table    = null;
    int                  idx      = 0;
    String               stmt     = null;

      stmt = "select username, granted_role, admin_option from user_role_privs order by username, granted_role";
          
    /* Select from user_role_privs */
    sel_stmt = db_conn.prepareCall (stmt);

    ResultSet rset = sel_stmt.executeQuery ();

    System.out.println ("\n[ User_roles ] ");
    while (rset.next ())
    {
      // System.out.print (rset.getString (1));
      // System.out.print("\t  " + rset.getString (2));
      // System.out.println ("\t  " + rset.getString (3));
      System.out.print (rpadString(rset.getString (1), 30));
      System.out.print (rpadString(rset.getString (2), 30));
      System.out.println ("  " + rset.getString (3));
    }

    System.out.print("\n");
    sel_stmt.close();
  }

  //==========================================================================
  public static void printAgentList(AQAgent[] agt_list)
                                    throws SQLException
  {
    int   i = 0;

    if(agt_list != null)
      System.out.println("Agent List : " + agt_list.length);
    else
    {
      System.out.println("Agent List : Null");
      return;
    }

    for (i = 0; i < agt_list.length; i++)
    {
      System.out.println("Agent:  " + agt_list[i].getName());
      System.out.println("        " + agt_list[i].getAddress() +
			 "    "     + agt_list[i].getProtocol());
    }

    System.out.println("");
  }


  //==========================================================================
  public static void printRecipientList(Vector recp_list)
                                        throws SQLException
  {
    int      i   = 0;
    AQAgent  agt = null;

    if(recp_list != null)
      System.out.println("Recipient List : " + recp_list.size());
    else
    {
      System.out.println("Recipient List : Null");
      return;
    }

    for (i = 0; i < recp_list.size(); i++)
    {
      agt = (AQAgent)(recp_list.elementAt(i));

      if(agt != null)
      {
	System.out.println("Agent:  " + agt.getName());
	System.out.println("        " + agt.getAddress() +
			   "    "     + agt.getProtocol());
      }
    }

    System.out.println("");
  }

/**
   * Print the content of the byte array in Hex
   *
   * @param     b    byte array
   *
   */
  public static String ByteArraytoString (byte [] b)
  {
    StringBuffer buf = new StringBuffer();
    int i;

    if (b == null)
      buf.append("<Null byte array!>");
    else
    {
      int n = b.length;
      for (i = 0; i < n; i++)
      {
	int v = b [i] & 0xff;
	if (v < 0x10)
	  buf.append("0" + Integer.toHexString (v));
	else
	  buf.append (Integer.toHexString (v));
      }
    }

    return buf.toString();
  }


  /**
   * Returns s rpadded to size n with blanks
   *
   * s 		the input string
   * n          desired length
   * @return    new string like s + blanks
   *
   */
  public static String rpadString(String s, int n)
  {
    int          i;
    int          l;
    StringBuffer buf;

    buf = new StringBuffer(n);
    if ( s != null)
       l = s.length();
    else 
       l=0;
    if ( l > n )
    {
       buf.append(s.substring(0,n));
    }
    else
    {
       buf.append(s);
       for ( i = l ; i < n ; i++ )
       {
          buf.append(" ");
       }
    }
 
    return buf.toString();
  }
 
  //
  // Register LDAP connection 
  // args - sid, host, port , driver
  // type - "QUEUE", "TOPIC"
  // conn_name - Is the connection name stored in LDAP dir.
  //
  public static void regConnection(String[] args, String type, String conn_name)
    throws SQLException,JMSException, ClassNotFoundException
  {  
     if (type.toLowerCase().equals("topic") )
         regTopicConnection(args[0], args[1], args[2], args[3],  conn_name) ;
     else if (type.toLowerCase().equals("queue"))
         regQueueConnection(args[0], args[1], args[2], args[3],  conn_name) ;
     else 
         System.out.println("Invalid Type specified") ;
  } 

  public static void regQueueConnection(String sid, String host, 
				    String port, String driver, String conn_name)

    throws SQLException,JMSException, ClassNotFoundException
  {  
     Connection conn    = null;
     conn = tkaqjutl.connectSys(null) ;
     AQjmsFactory.registerConnectionFactory(conn, conn_name, host, sid, 
                                      Integer.parseInt(port), driver, "QUEUE") ;
     conn.close();
  } 

  public static void regTopicConnection(String sid, String host, String port, 
                                  String driver, String conn_name)
    throws SQLException,JMSException, ClassNotFoundException
  {  
     Connection conn    = null;

       conn = tkaqjutl.connectSys(null) ;
       AQjmsFactory.registerConnectionFactory(conn, conn_name, host, sid, 
                                      Integer.parseInt(port), driver, "TOPIC") ;
       conn.close();
  } 
  
   public static CLOB fillCLOB ( CLOB clob, long length, long piece)
    throws Exception
  {
    CLOB cl = clob;
    try
    {
      long i = 1;
      long chunk = piece;
      String pc = "";
      String ch = "A";

      while (pc.length() < piece) {
        pc = pc + ch;
      }

      System.out.println ("piece of length " + piece);
      while (i < length) {
        System.out.println ("Writing piece of length " + pc.length() +
                            ", chunk of length " + chunk);
        int iLen = clob.putString( i, pc );
        System.out.println("Wrote piece of length " + iLen);
        i += chunk;
        if (length - i < chunk)
          chunk = length - i;
      }
    } finally {}
    return clob;
    
  }

    // Utility function to put data in a BLOB
  public static BLOB fillBLOB (BLOB blob, long length, long piece)
    throws Exception
  {
    BLOB oraBlob = blob;
    try
    {
      long i = 0;
      long chunk = piece;
      int j = 0;

      if ( piece > 32512 || piece < 1)
        piece = 32512;

      byte [] data = new byte [(int)piece];

      while ( j < piece ) {
        data [j] = (byte) (j % 10);
        j++;
      }
      System.out.println ("LOB size, piece of length = " + length +" "+ piece);
      i = 1;
      chunk = piece;

      while (i < length)
      {
        System.out.println ("Writing piece of length " + piece +
                            ", chunk of length " + chunk);
        int ilen = oraBlob.putBytes(i, data);
        i += (long)ilen;
        if (length - i < chunk)
          chunk = length - i;
      }
    } finally {} 
    return oraBlob;
    
  }
  public static void dumpBLOB ( BLOB blob, long chunk)
    throws Exception
  {

    BLOB oraBlob = blob;
    try
    {
      long length = oraBlob.length();
      long i = 1;
      System.out.println ("Lob length = " + length);
      while (i < length)
      {
        byte [] bytes_this_time = new byte[32768];

        int read_this_time = oraBlob.getBytes( i, (int)chunk, bytes_this_time);
        System.out.println ("Read " + read_this_time + " bytes: ");
        int j;
        for (j = 0; j < read_this_time; j++) {
          if ((j > 0 ) && ((j % 40) == 0)) System.out.println("") ;
          System.out.print (bytes_this_time [j] + "");
        }
        System.out.println ("");
        i += read_this_time;
      }
    } finally {}
  } 
  /* 
   * Enable/Disable Debug tracing 
   */
  public static void setDebug(boolean val)
   {
      if (val)
      {
        AQOracleDebug.setTraceLevel(5);
        AQOracleDebug.setDebug(true);
      }
   }

  /* 
   * Print a sorted list of keys for an Enumeration
   */
   public static String printSortedEnumeration(Enumeration enum1, String header)
   {
     String key = null;
     Vector sortedKeys = new Vector();
     while (enum1.hasMoreElements())
     {
       key = (String) enum1.nextElement();

       boolean insertDone = false;
       int i = 0;
       if (sortedKeys.size() == 0)
         sortedKeys.addElement(key);
       else
         while (!insertDone && (i < sortedKeys.size()))
         {
           if (key.compareTo((String)sortedKeys.elementAt(i)) < 0)
           {
             insertDone = true;
             sortedKeys.insertElementAt(key,i);
           }
           else if ((i+1) == sortedKeys.size())
           {
             insertDone = true;
             sortedKeys.addElement(key);
           }
           else
             i++;
         }
     }
     String rtnString = "["+header+"]\n[";
     for (int v=0; v < sortedKeys.size(); v++)
     {
       rtnString = rtnString + ((String)sortedKeys.elementAt(v));
       if ((v+1) < sortedKeys.size())
       {
         rtnString = rtnString + ",";
         if ((v % 3) == 0)
           rtnString = rtnString + "\n";
       }
     }
     return rtnString + "]\n\n";
  }


    public static String genText(char c, int count) {
        char[] data = new char[count];
        for (int i = 0; i < count; i++) {
            data[i] = c;
        }

        return new String(data);
    }


    public static byte[] genBytes(byte b, int count) {
        byte[] data = new byte[count];
        for (int i = 0; i < count; i++) {
            data[i] = b;
        }

        return data;
    }
}
