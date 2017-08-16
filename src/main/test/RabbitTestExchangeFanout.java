import com.rabbitmq.client.*;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created by haoxi on 2017/8/15.
 */
public class RabbitTestExchangeFanout {
    public static final String QUEUE_NAME = "order_queue_7";
    public static final String EXCHANGE_NAME = "logs";

    @Test
    public void testProvider() {
        try {
            ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.setHost("localhost");
            Connection connection = connectionFactory.newConnection();
            Channel channel = connection.createChannel();
            //channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
            String message1 = "hello world!";
            String message2 = "ronaldo!";
            channel.basicPublish(EXCHANGE_NAME, "", null, message1.getBytes());
            channel.basicPublish(EXCHANGE_NAME, "", null, message2.getBytes());
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
            connectionFactory.setHost("localhost");
            Connection connection = connectionFactory.newConnection();
            final Channel channel = connection.createChannel();

            /**
             * 因为发送的是fanout，所以所有的队列都会收到消息<br>
             *     因此可以使用随机的队列名称
             */
            String queueName = channel.queueDeclare().getQueue();//随机的队列名称
            channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
            channel.queueBind(queueName, EXCHANGE_NAME, "");
            //消费队列，自动ack，消费者
            //boolean autoAck = true;
            //channel.basicConsume(queueName, autoAck, new DefaultConsumer(channel) {
            //    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
            //        String message = new String(body, "UTF-8");
            //        System.out.println(" [x] Received '" + message + "'");
            //    }
            //});

            QueueingConsumer consumer = new QueueingConsumer(channel);
            channel.basicConsume(queueName, true, consumer);

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
            connectionFactory.setHost("localhost");
            Connection connection = connectionFactory.newConnection();
            final Channel channel = connection.createChannel();

            String queueName = channel.queueDeclare().getQueue();//随机的队列名称
            channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
            channel.queueBind(queueName, EXCHANGE_NAME, "");
            //消费队列，自动ack，消费者
            //boolean autoAck = true;
            //channel.basicConsume(queueName, autoAck, new DefaultConsumer(channel) {
            //    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
            //        String message = new String(body, "UTF-8");
            //        System.out.println(" [x] Received '" + message + "'");
            //    }
            //});

            QueueingConsumer consumer = new QueueingConsumer(channel);
            channel.basicConsume(queueName, true, consumer);

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
