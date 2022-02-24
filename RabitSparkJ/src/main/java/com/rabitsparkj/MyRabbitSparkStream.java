package com.rabitsparkj;

import com.rabbitmq.client.ConnectionFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class MyRabbitSparkStream {
    public static void main(String[] args) throws InterruptedException {
        JavaStreamingContext context=new JavaStreamingContext(new SparkConf().setAppName("Do it"),new Duration(5000));
        JavaDStream<String> customReceiverStream = context.receiverStream(new RabbitReceiver("test"));
        customReceiverStream.print();
        context.start();
        context.awaitTermination();
    }
}
