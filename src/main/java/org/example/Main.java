package org.example;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageProducer;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerService;

import javax.jms.*;


public class Main {
    private static final String HOST = "tcp://localhost:61616";
    private static final String USER = "admin";
    private static final String PASSWORD = "admin";

    public static void main(String[] args) throws Exception {
        /*
        Broker broker = startEmbeddedBroker().getBroker();
        System.out.println(broker.getBrokerName());
         */
        provider();
        customer();
    }

    public static Connection getConnection() throws JMSException {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(USER, PASSWORD, HOST);
        Connection connection = connectionFactory.createConnection();
        connection.start();
        return connection;
    }

    //providers, maybe need to move to separate package

    //auto ack non-persistent provider with standard priority
    private static void provider() {
        try (Connection connection = getConnection()) {
            Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue("auto ack queue");
            MessageProducer producer = session.createProducer(destination);
            TextMessage message = session.createTextMessage();
            message.setText("This is test message from .provider()");
            producer.send(destination, message, DeliveryMode.NON_PERSISTENT, 4, 1000*10);
        } catch (JMSException jmsException) {
            //add log4j error record
        }
    }

    //customers, maybe need to move to separate package

    //auto ack customer
    public static void customer() throws JMSException {
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

    /*
    private static BrokerService startEmbeddedBroker() throws Exception {
        BrokerService brokerService = new BrokerService();
        brokerService.addConnector("tcp://localhost:61616");
        brokerService.setBrokerName("test broker instance");
        brokerService.start();
        System.out.println("Started Embedded Broker");
        return brokerService;
    }
     */
}