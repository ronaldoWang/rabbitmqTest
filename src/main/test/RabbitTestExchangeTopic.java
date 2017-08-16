import com.rabbitmq.client.*;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created by haoxi on 2017/8/15.
 */
public class RabbitTestExchangeTopic {
    public static final String QUEUE_NAME10 = "order_queue_10";
    public static final String QUEUE_NAME11 = "order_queue_11";
    public static final String EXCHANGE_NAME = "logs_topic";

    public static final String URL = "192.168.1.5";
    public static final String username = "admin";
    public static final String password = "admin";
    public static final Integer port = 5672;

    @Test
    public void testProvider() {
        try {
            ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.setHost(URL);
            connectionFactory.setUsername(username);
            connectionFactory.setPassword(password);
            connectionFactory.setPort(port);
            Connection connection = connectionFactory.newConnection();
            Channel channel = connection.createChannel();
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
            String message1 = "hello world!";
            String message2 = "ronaldo!";
            channel.basicPublish(EXCHANGE_NAME, "quick.orange.rabbit", null, message1.getBytes());
            channel.basicPublish(EXCHANGE_NAME, "lazy.orange.male.rabbit", null, message2.getBytes());
            System.out.println(" [x] Sent '" + message1 + "'");
            System.out.println(" [x] Sent '" + message2 + "'");
            // 关闭频道和连接
            channel.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testConsumer1() {
        try {
            ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.setHost(URL);
            connectionFactory.setUsername(username);
            connectionFactory.setPassword(password);
            connectionFactory.setPort(port);
            Connection connection = connectionFactory.newConnection();
            final Channel channel = connection.createChannel();

            channel.queueDeclare(QUEUE_NAME10, false, false, false, null);
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
            channel.queueBind(QUEUE_NAME10, EXCHANGE_NAME, "*.*.rabbit");
            QueueingConsumer consumer = new QueueingConsumer(channel);
            channel.basicConsume(QUEUE_NAME10, true, consumer);

        /* 读取队列，并且阻塞，即在读到消息之前在这里阻塞，直到等到消息，完成消息的阅读后，继续阻塞循环 */
            while (true) {
                QueueingConsumer.Delivery delivery = consumer.nextDelivery();
                String message = new String(delivery.getBody());
                System.out.println("testConsumer1收到消息'" + message + "'");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testConsumer2() {
        try {
            ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.setHost(URL);
            connectionFactory.setUsername(username);
            connectionFactory.setPassword(password);
            connectionFactory.setPort(port);
            Connection connection = connectionFactory.newConnection();
            final Channel channel = connection.createChannel();

            channel.queueDeclare(QUEUE_NAME11, false, false, false, null);
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
            channel.queueBind(QUEUE_NAME11, EXCHANGE_NAME, "lazy.#");
            QueueingConsumer consumer = new QueueingConsumer(channel);
            channel.basicConsume(QUEUE_NAME11, true, consumer);

        /* 读取队列，并且阻塞，即在读到消息之前在这里阻塞，直到等到消息，完成消息的阅读后，继续阻塞循环 */
            while (true) {
                QueueingConsumer.Delivery delivery = consumer.nextDelivery();
                String message = new String(delivery.getBody());
                System.out.println("testConsumer2收到消息'" + message + "'");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
