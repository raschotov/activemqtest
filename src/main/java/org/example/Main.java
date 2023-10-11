package org.example;

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

    //providers, maybe need to move to separate package
    //auto ack non-persistent provider with standard priority
    private static void autoAckProvider() {
        try (Connection connection = getConnection()) {
            Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue("auto ack queue");
            MessageProducer producer = session.createProducer(destination);
            TextMessage message = session.createTextMessage();
            message.setText("This is auto ack message");
            producer.send(destination, message, DeliveryMode.NON_PERSISTENT, 4, 1000*10);
        } catch (JMSException jmsException) {
            //add log4j error record
        }
    }

    private static void clientAckProvider() {
        try (Connection connection = getConnection()) {
            Session session = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
            Destination destination = session.createQueue("client ack queue");
            MessageProducer producer = session.createProducer(destination);
            TextMessage message = session.createTextMessage();
            message.setText("This is client ack message");
            producer.send(destination, message, DeliveryMode.NON_PERSISTENT, 4, 1000*10);
        } catch (JMSException jmsException) {
            //add log4j error record
        }
    }

    private static void dupsOkAckProvider() {
        try (Connection connection = getConnection()) {
            Session session = connection.createSession(true, Session.DUPS_OK_ACKNOWLEDGE);
            Destination destination = session.createQueue("dups ok ack queue");
            MessageProducer producer = session.createProducer(destination);
            TextMessage message = session.createTextMessage();
            message.setText("This is dups ok message");
            producer.send(destination, message, DeliveryMode.NON_PERSISTENT, 4, 1000*10);
        } catch (JMSException jmsException) {
            //add log4j error record
        }
    }

    private static void sessionTransactedAckProvider() {
        try (Connection connection = getConnection()) {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Destination destination = session.createQueue("session transacted ack queue");
            MessageProducer producer = session.createProducer(destination);
            TextMessage message = session.createTextMessage();
            message.setText("This is session transacted message");
            producer.send(destination, message, DeliveryMode.NON_PERSISTENT, 4, 1000*10);
        } catch (JMSException jmsException) {
            //add log4j error record
        }
    }

    //customers, maybe need to move to separate package
    //auto ack customer
    public static void autoAckCustomer() throws JMSException {
        try (Connection connection = getConnection()) {
            Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue("auto ack queue");
            MessageConsumer consumer = session.createConsumer(destination);
            TextMessage message = (TextMessage) consumer.receive();
            System.out.println(message.getText());
        } catch (JMSException jmsException) {
            //add log4j error record
        }
    }

    public static void clientAckCustomer() throws JMSException {
        try (Connection connection = getConnection()) {
            Session session = connection.createSession(true, Session.CLIENT_ACKNOWLEDGE);
            Destination destination = session.createQueue("client ack queue");
            MessageConsumer consumer = session.createConsumer(destination);
            TextMessage message = (TextMessage) consumer.receive();
            System.out.println(message.getText());
        } catch (JMSException jmsException) {
            //add log4j error record
        }
    }

    public static void dupsOkAckCustomer() throws JMSException {
        try (Connection connection = getConnection()) {
            Session session = connection.createSession(true, Session.DUPS_OK_ACKNOWLEDGE);
            Destination destination = session.createQueue("dups ok ack queue");
            MessageConsumer consumer = session.createConsumer(destination);
            TextMessage message = (TextMessage) consumer.receive();
            System.out.println(message.getText());
        } catch (JMSException jmsException) {
            //add log4j error record
        }
    }

    public static void sessionTransactedAckCustomer() throws JMSException {
        try (Connection connection = getConnection()) {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Destination destination = session.createQueue("session transacted ack queue");
            MessageConsumer consumer = session.createConsumer(destination);
            TextMessage message = (TextMessage) consumer.receive();
            System.out.println(message.getText());
        } catch (JMSException jmsException) {
            //add log4j error record
        }
    }
}