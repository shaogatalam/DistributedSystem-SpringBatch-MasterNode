package BasePack.service;

import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
@Service
public class QueueService {
    private final AmqpAdmin amqpAdmin;

    @Autowired
    public QueueService(AmqpAdmin amqpAdmin) {
        this.amqpAdmin = amqpAdmin;
    }

    public void createQueue(String queueName, boolean durable) {
        Queue queue = new Queue(queueName, durable);
        amqpAdmin.declareQueue(queue);
        System.out.println("Queue created: " + queueName);
    }
}
