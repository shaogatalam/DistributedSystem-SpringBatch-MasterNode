package BasePack.Config;
import org.springframework.amqp.core.Queue;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class MasterConfig {

    private final JobLauncher jobLauncher;
    private final Job job;

    public MasterConfig(JobLauncher jobLauncher, Job job) {
        this.jobLauncher = jobLauncher;
        this.job = job;
    }
    @Bean
    public CommandLineRunner startJob() {
        return args -> {
            try {
                JobParameters jobParameters = new JobParametersBuilder()
                        .addLong("time", System.currentTimeMillis()) // to ensure uniqueness
                        .toJobParameters();
                jobLauncher.run(job, jobParameters);
                System.out.println("Batch job has been started automatically.");
            } catch (Exception e) {
                System.err.println("Job failed to start: " + e.getMessage());
                e.printStackTrace();
            }
        };
    }

    @Bean
    public Queue repliesQueue() {
        return new Queue("replies", true);
    }

    @Bean
    public Queue requestsQueue() {
        return new Queue("requests", true);
    }

    @Bean
    public Queue emailQueue() {
        return new Queue("emailQueue", true);
    }

}

