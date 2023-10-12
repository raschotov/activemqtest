package org.example;

import java.io.File;
import java.util.HashMap;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.security.SimpleAuthenticationPlugin;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;


public class Main {
    private static final String HOST = "tcp://localhost:61616";
    private static final String USER = "admin";
    private static final String PASSWORD = "admin";

    public static void main(String[] args) throws Exception {
        BrokerService broker = getEmbeddedBroker();
        broker.start();
        System.out.println("Broker started");

        autoAckProvider();
        getBrokerConnectionsCount(broker);

        autoAckCustomer(); //stops here
        getBrokerConnectionsCount(broker);

        clientAckProvider();
        clientAckCustomer();
        getBrokerConnectionsCount(broker);

        dupsOkAckProvider();
        dupsOkAckCustomer();
        getBrokerConnectionsCount(broker);

        sessionTransactedAckProvider();
        sessionTransactedAckCustomer();
        getBrokerConnectionsCount(broker);

        //interruption may cause HOST port locked to keep in use until JVM kill
        broker.stop();
        System.out.println("Broker stopped");
    }

    public static Connection getConnection() throws JMSException {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(USER, PASSWORD, HOST);
        Connection connection = connectionFactory.createConnection();
        connection.start();
        return connection;
    }

    //Need to rewrite to work properly
    public static BrokerService getEmbeddedBroker() throws Exception {
        BrokerService broker = new BrokerService();
        KahaDBPersistenceAdapter adaptor = new KahaDBPersistenceAdapter();
        adaptor.setDirectory(new File("activemq"));
        broker.setPersistenceAdapter(adaptor);

        HashMap<String, String> userPasswords = new HashMap<>();
        userPasswords.put(USER, PASSWORD);
        SimpleAuthenticationPlugin simpleAuthenticationPlugin = new SimpleAuthenticationPlugin();
        simpleAuthenticationPlugin.setUserPasswords(userPasswords);
        broker.setPlugins(new BrokerPlugin[]{simpleAuthenticationPlugin});

        broker.setUseJmx(true);
        broker.addConnector(HOST);
        return broker;
    }

    public static void getBrokerConnectionsCount(BrokerService broker) {
        System.out.printf("Количество подключений: %d ", broker.getCurrentConnections());
    }

    //providers, maybe need to move to separate package
    //auto ack non-persistent provider with standard priority
    private static void autoAckProvider() throws JMSException {
        Connection connection = null;
        try {
            connection = getConnection();
            Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue("auto");
            MessageProducer producer = session.createProducer(destination);
            TextMessage message = session.createTextMessage();
            message.setText("This is auto ack message");
            producer.send(destination, message, DeliveryMode.NON_PERSISTENT, 4, 1000*10);
            
        } catch (JMSException jmsException) {
            System.out.println("connection error");
            //add log4j error record
        } finally {
            if (connection != null) connection.close();
        }
    }

    private static void clientAckProvider() throws JMSException {
        Connection connection = null;
        try {
            connection = getConnection();
            Session session = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
            Destination destination = session.createQueue("client");
            MessageProducer producer = session.createProducer(destination);
            TextMessage message = session.createTextMessage();
            message.setText("This is client ack message");
            producer.send(destination, message, DeliveryMode.NON_PERSISTENT, 4, 1000*10);
        } catch (JMSException jmsException) {
            System.out.println("connection error");
            //add log4j error record
        } finally {
            if (connection != null) connection.close();
        }
    }

    private static void dupsOkAckProvider() throws JMSException {
        Connection connection = null;
        try {
            connection = getConnection();
            Session session = connection.createSession(true, Session.DUPS_OK_ACKNOWLEDGE);
            Destination destination = session.createQueue("dups");
            MessageProducer producer = session.createProducer(destination);
            TextMessage message = session.createTextMessage();
            message.setText("This is dups ok message");
            producer.send(destination, message, DeliveryMode.NON_PERSISTENT, 4, 1000*10);
        } catch (JMSException jmsException) {
            System.out.println("connection error");
            //add log4j error record
        } finally {
            if (connection != null) connection.close();
        }
    }

    private static void sessionTransactedAckProvider() throws JMSException {
        Connection connection = null;
        try {
            connection = getConnection();
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Destination destination = session.createQueue("session");
            MessageProducer producer = session.createProducer(destination);
            TextMessage message = session.createTextMessage();
            message.setText("This is session transacted message");
            producer.send(destination, message, DeliveryMode.NON_PERSISTENT, 4, 1000*10);
        } catch (JMSException jmsException) {
            System.out.println("connection error");
            //add log4j error record
        } finally {
            if (connection != null) connection.close();
        }
    }

    //customers, maybe need to move to separate package
    //auto ack customer
    public static void autoAckCustomer() throws JMSException {
        Connection connection = null;
        try {
            connection = getConnection();
            Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue("auto"); //error here
            MessageConsumer consumer = session.createConsumer(destination);
            TextMessage message = (TextMessage) consumer.receive();
            System.out.println(message.getText());
        } catch (JMSException jmsException) {
            System.out.println("connection error");
            //add log4j error record
        } finally {
            if (connection != null) connection.close();
        }
    }

    public static void clientAckCustomer() throws JMSException {
        Connection connection = null;
        try {
            connection = getConnection();
            Session session = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
            Destination destination = session.createQueue("client");
            MessageConsumer consumer = session.createConsumer(destination);
            TextMessage message = (TextMessage) consumer.receive();
            System.out.println(message.getText());
        } catch (JMSException jmsException) {
            System.out.println("connection error");
            //add log4j error record
        } finally {
            if (connection != null) connection.close();
        }
    }

    public static void dupsOkAckCustomer() throws JMSException {
        Connection connection = null;
        try {
            connection = getConnection();
            Session session = connection.createSession(true, Session.DUPS_OK_ACKNOWLEDGE);
            Destination destination = session.createQueue("dups");
            MessageConsumer consumer = session.createConsumer(destination);
            TextMessage message = (TextMessage) consumer.receive();
            System.out.println(message.getText());
        } catch (JMSException jmsException) {
            System.out.println("connection error");
            //add log4j error record
        } finally {
            if (connection != null) connection.close();
        }
    }

    public static void sessionTransactedAckCustomer() throws JMSException {
        Connection connection = null;
        try {
            connection = getConnection();
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Destination destination = session.createQueue("session");
            MessageConsumer consumer = session.createConsumer(destination);
            TextMessage message = (TextMessage) consumer.receive();
            System.out.println(message.getText());
        } catch (JMSException jmsException) {
            System.out.println("connection error");
            //add log4j error record
        } finally {
            if (connection != null) connection.close();
        }
    }
}