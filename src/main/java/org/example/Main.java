package org.example;

import java.io.File;

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
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;


public class Main {
    private static final String HOST = "tcp://localhost:61616";
    private static final String USER = "admin";
    private static final String PASSWORD = "admin";

    public static void main(String[] args) throws Exception {
        autoAckProvider();
        autoAckCustomer();

        clientAckProvider();
        clientAckCustomer();

        dupsOkAckProvider();
        dupsOkAckCustomer();

        sessionTransactedAckProvider();
        sessionTransactedAckCustomer();
    }

    public static Connection getConnection() throws JMSException {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(USER, PASSWORD, HOST);
        Connection connection = connectionFactory.createConnection();
        connection.start();
        return connection;
    }

    //Need to rewrite to work properly
    public static void startEmbeddedBroker() throws Exception {
        BrokerService broker = new BrokerService();
        KahaDBPersistenceAdapter adaptor = new KahaDBPersistenceAdapter();
        adaptor.setDirectory(new File("activemq"));
        broker.setPersistenceAdapter(adaptor);
        broker.setUseJmx(true);
        broker.addConnector("tcp://localhost:61616");
        broker.start();
    }

    //providers, maybe need to move to separate package
    //auto ack non-persistent provider with standard priority
    private static void autoAckProvider() throws JMSException {
        Connection connection = null;
        try {
            connection = getConnection();
            Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue("auto ack queue");
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
            Destination destination = session.createQueue("client ack queue");
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
            Destination destination = session.createQueue("dups ok ack queue");
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
            Destination destination = session.createQueue("session transacted ack queue");
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
            Destination destination = session.createQueue("auto ack queue");
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
            Destination destination = session.createQueue("client ack queue");
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
            Destination destination = session.createQueue("dups ok ack queue");
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
            Destination destination = session.createQueue("session transacted ack queue");
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