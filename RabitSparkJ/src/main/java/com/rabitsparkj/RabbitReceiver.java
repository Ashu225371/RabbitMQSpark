package com.rabitsparkj;


import hello.ConsumerMessageListener;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.util.Properties;
import java.util.Random;

public class RabbitReceiver extends Receiver<String> implements MessageListener{

    String queName;

    public RabbitReceiver(String queueName) {
        super(StorageLevel.MEMORY_AND_DISK_SER_2());
        this.queName=queueName;
    }

    public void onStart() {

        new Thread(this::receive).start();

    }

    private void receive() {
        try
        {
            Properties properties=new Properties();
            properties.setProperty(Context.INITIAL_CONTEXT_FACTORY,"com.sun.jndi.fscontext.RefFSContextFactory");
            properties.setProperty(Context.PROVIDER_URL, "file:C:/xxxx/zaaaa/test");
            InitialContext initialContext= new InitialContext(properties);
            QueueConnectionFactory factory=(QueueConnectionFactory) initialContext.lookup("ConnectionFactory");
            QueueConnection connection=factory.createQueueConnection();
            QueueSession session = connection.createQueueSession(false, Session.DUPS_OK_ACKNOWLEDGE);
            Queue queue = session.createQueue(queName);
            QueueReceiver queueReceiver = session.createReceiver(queue);
            queueReceiver.setMessageListener(this);
            connection.start();
        }
        catch(Exception ex)
        {
            ex.printStackTrace();
        }
   }

    public void onStop() {


    }

    @Override
    public void onMessage(Message message) {
        TextMessage textMessage = (TextMessage) message;
        try {
            //System.out.println(" received " + textMessage.getText());
            store(textMessage.getText());
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }
}
