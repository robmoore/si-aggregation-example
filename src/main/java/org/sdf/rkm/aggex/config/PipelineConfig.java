package org.sdf.rkm.aggex.config;

import org.sdf.rkm.aggex.domain.TimeDiff;
import org.sdf.rkm.aggex.domain.TimeResponse;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.http.MediaType;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.amqp.Amqp;
import org.springframework.integration.dsl.http.Http;
import org.springframework.integration.dsl.support.Transformers;
import org.springframework.integration.redis.store.RedisMessageStore;
import org.springframework.integration.redis.util.RedisLockRegistry;
import org.springframework.integration.store.MessageGroupStore;
import org.springframework.integration.store.MessageGroupStoreReaper;
import org.springframework.integration.support.locks.LockRegistry;
import org.springframework.web.client.RestTemplate;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Configuration
public class PipelineConfig {
    @Bean
    public IntegrationFlow timeRequestHttpInboundGatewayFlow(AmqpTemplate amqpTemplate) {
        return IntegrationFlows.from(Http.inboundChannelAdapter("/start"))
                .log()
                .handle((payload, headers) -> IntStream.range(1, 11).boxed().collect(Collectors.toSet()))
                .split()
                .handle(Amqp.outboundAdapter(amqpTemplate).routingKey(timeQueryQueue().getName()))
                .get();
    }

    @Bean
    public IntegrationFlow timeRequestFlow(ConnectionFactory connectionFactory, AmqpTemplate amqpTemplate,
                                           MessageGroupStore messageGroupStore, LockRegistry lockRegistry) {
        RestTemplate restTemplate = new RestTemplate();
        return IntegrationFlows.from(Amqp.inboundAdapter(connectionFactory, timeQueryQueue()))
                .log()
                .handle((payload, headers) -> {
                    int delay = ThreadLocalRandom.current().nextInt(1000, 10000);
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    return restTemplate.getForObject("https://now.httpbin.org", TimeResponse.class);
                })
                .aggregate(a -> a.messageStore(messageGroupStore).lockRegistry(lockRegistry))
                .<List<TimeResponse>, List<TimeDiff>>transform(source -> {
                    List<ZonedDateTime> zonedDateTimes = source.stream()
                            .map(timeResponse -> ZonedDateTime.parse(timeResponse.getNow().get("iso8601").toString()))
                            .collect(Collectors.toList());
                    Double epochSecondAverage = zonedDateTimes.stream()
                            .collect(Collectors.averagingLong(ZonedDateTime::toEpochSecond));
                    return zonedDateTimes.stream().map(zonedDateTime -> new TimeDiff(zonedDateTime.toEpochSecond(),
                            epochSecondAverage)).collect(Collectors.toList());
                })
                .split()
                .handle(Amqp.outboundAdapter(amqpTemplate).routingKey(timeNotifyQueue().getName()))
                .get();
    }

    @Bean
    public IntegrationFlow timeNotifyFlow(ConnectionFactory connectionFactory) {
        return IntegrationFlows.from(Amqp.inboundAdapter(connectionFactory, timeNotifyQueue()))
                .log()
                .transform(Transformers.toJson(MediaType.APPLICATION_JSON_VALUE))
                .handleWithAdapter(a ->
                        a.httpGateway("https://httpbin.org/post"))
                .channel("nullChannel")
                .get();
    }

    @Bean
    public MessageGroupStore messageGroupStore(RedisConnectionFactory redisConnectionFactory) {
        //return new SimpleMessageStore();
        return new RedisMessageStore(redisConnectionFactory, "time-query-msg_"); //redisMessageStore;
    }

    @Bean
    public LockRegistry lockRegistry(RedisConnectionFactory redisConnectionFactory) {
        //return new DefaultLockRegistry();
        return new RedisLockRegistry(redisConnectionFactory, "time-query-lock_");
    }

    @Bean
    public Queue timeQueryQueue() {
        return QueueBuilder.durable("time-query").build();
    }

    @Bean
    public Queue timeNotifyQueue() {
        return QueueBuilder.durable("time-notify").build();
    }

    @Bean
    public MessageGroupStoreReaper messageGroupStoreReaper(MessageGroupStore messageGroupStore) {
        MessageGroupStoreReaper reaper = new MessageGroupStoreReaper(messageGroupStore);
        reaper.setTimeout(60000 * 10);
        return reaper;
    }
}
