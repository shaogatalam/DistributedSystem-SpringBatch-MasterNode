package BasePack;

//import com.rabbitmq.client.ConnectionFactory;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.integration.partition.RemotePartitioningManagerStepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.dsl.DirectChannelSpec;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
public class BatchMasterApplication {

	public static void main(String[] args) {
		SpringApplication.run(BatchMasterApplication.class, args);
	}


	@Bean
	DirectChannelSpec CustomRequestsChannel() {
		return MessageChannels.direct();
	}
	@Bean
	IntegrationFlow requestsFlow(MessageChannel CustomRequestsChannel, AmqpTemplate amqpTemplate) {
		return IntegrationFlow.from(CustomRequestsChannel).handle(Amqp.outboundAdapter(amqpTemplate).routingKey("requests")).get();
	}


	@Bean
	DirectChannelSpec CustomRepliesChannel() {
		return MessageChannels.direct();
	}
	@Bean
	IntegrationFlow repliesFlow(MessageChannel CustomRepliesChannel, ConnectionFactory connectionFactory ) {
		var smc = new SimpleMessageConverter();
		smc.addAllowedListPatterns("*");
		return IntegrationFlow.from(Amqp.inboundAdapter(connectionFactory, "replies").messageConverter(smc)).channel(CustomRepliesChannel).get();
	}

	@Bean
	Job job(JobRepository repository, Step managerStep) {
		return new JobBuilder("job", repository)
				.start(managerStep)
				.build();
	}


	@Bean
	Step managerStep(JobRepository repository, BeanFactory beanFactory,
					 JobExplorer explorer,SpringTipsPartitioner partitioner,
					 MessageChannel CustomRequestsChannel,MessageChannel CustomRepliesChannel){
		var gridSize = 4;
		return new RemotePartitioningManagerStepBuilder("managerStep",repository)
				.partitioner("workerStep",partitioner)
				.beanFactory(beanFactory)
				.gridSize(gridSize)
				.outputChannel(CustomRequestsChannel)
				.inputChannel(CustomRepliesChannel)
				.jobExplorer(explorer)
				.build();
	}




	@Component
	static class SpringTipsPartitioner implements Partitioner {

		@Override
		public Map<String, ExecutionContext> partition(int gridSize) {

			Map<String, ExecutionContext> partitions = new HashMap<>();

			int totalCustomers = 20; // Fetch this value from the database
			int partitionSize = totalCustomers / gridSize;

			for (int i = 0; i < gridSize; i++) {
				ExecutionContext context = new ExecutionContext();
				int startId = i * partitionSize + 1;
				int endId = (i + 1) * partitionSize;
				if (i == gridSize - 1) {
					endId = totalCustomers;
				}
				context.putInt("startId", startId);
				context.putInt("endId", endId);
				partitions.put("partition" + i, context);
			}
			return partitions;
		}
	}


}
