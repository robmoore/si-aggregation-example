package org.sdf.rkm.aggex.config;

import org.sdf.rkm.aggex.domain.TimeDiff;
import org.sdf.rkm.aggex.domain.TimeResponse;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.http.MediaType;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.amqp.Amqp;
import org.springframework.integration.dsl.core.Pollers;
import org.springframework.integration.dsl.http.Http;
import org.springframework.integration.dsl.support.Transformers;
import org.springframework.integration.redis.store.RedisMessageStore;
import org.springframework.integration.redis.util.RedisLockRegistry;
import org.springframework.integration.store.MessageGroupStore;
import org.springframework.integration.store.MessageGroupStoreReaper;
import org.springframework.integration.support.locks.LockRegistry;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.web.client.RestTemplate;

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Configuration
public class PipelineConfig implements BeanClassLoaderAware {
    private ClassLoader classLoader;

    @Bean
    public IntegrationFlow timeRequestHttpInboundGatewayFlow(AmqpTemplate amqpTemplate) {
        return IntegrationFlows.from(Http.inboundChannelAdapter("/start"))
                .log()
                .handle((payload, headers) -> IntStream.range(1, 11).boxed().collect(Collectors.toSet()))
                .split()
                .handle(Amqp.outboundAdapter(amqpTemplate).routingKey(timeHttpQueryQueue().getName()))
                .get();
    }

    @Bean
    public MessageSource<?> integerMessageSource() {
        Random r = new Random();

        return (MessageSource<List<Integer>>) () -> new Message<List<Integer>>() {
            @Override
            public List<Integer> getPayload() {
                return r.ints().boxed().limit(10).collect(Collectors.toList());
            }

            @Override
            public MessageHeaders getHeaders() {
                return new MessageHeaders(Collections.emptyMap());
            }
        };
    }

    @Bean
    public IntegrationFlow timeRequestPollingFlow(AmqpTemplate amqpTemplate) {
        return IntegrationFlows.from(integerMessageSource(),
                c -> c.poller(Pollers.fixedRate(600000, 30000).maxMessagesPerPoll(1)))
                .split()
                .handle(Amqp.outboundAdapter(amqpTemplate).routingKey(timePollQueryQueue().getName()))
                .get();
    }

    @Bean
    public IntegrationFlow timeRequestFlow(ConnectionFactory connectionFactory, AmqpTemplate amqpTemplate,
                                           MessageGroupStore messageGroupStore, LockRegistry lockRegistry) {
        RestTemplate restTemplate = new RestTemplate();
        return IntegrationFlows.from(Amqp.inboundAdapter(connectionFactory, timeHttpQueryQueue(), timePollQueryQueue()))
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
                .aggregate(a -> a.messageStore(messageGroupStore).lockRegistry(lockRegistry).expireGroupsUponCompletion(true))
                .<List<TimeResponse>, List<TimeDiff>>transform(source -> {
                    List<ZonedDateTime> zonedDateTimes = source.stream()
                            .map(timeResponse -> ZonedDateTime.parse(timeResponse.getNow().get("iso8601").toString()))
                            .collect(Collectors.toList());
                    Double epochSecondAverage = zonedDateTimes.stream()
                            .collect(Collectors.averagingLong(ZonedDateTime::toEpochSecond));
                    return zonedDateTimes.stream().map(zonedDateTime -> new TimeDiff(zonedDateTime.toEpochSecond(),
                            zonedDateTime.toEpochSecond() - epochSecondAverage)).collect(Collectors.toList());
                })
                .split()
                .handle(Amqp.outboundAdapter(amqpTemplate).routingKey(timeNotifyQueue().getName()))
                .get();
    }

//    @Bean
//    public MessageHandler logger() {
//        return new LoggingHandler(LoggingHandler.Level.INFO.name());
//    }
//
//    @Bean IntegrationFlow logFlow(ConnectionFactory connectionFactory) {
//        return IntegrationFlows.from(Amqp.inboundAdapter(connectionFactory, xxxQueue()))
//                .handle(logger())
//                .get();
//    }

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
        RedisMessageStore redisMessageStore = new RedisMessageStore(redisConnectionFactory, "time-query-msg_");
        redisMessageStore.setValueSerializer(new JdkSerializationRedisSerializer(this.classLoader));
        return redisMessageStore;
    }

    @Bean
    public LockRegistry lockRegistry(RedisConnectionFactory redisConnectionFactory) {
        return new RedisLockRegistry(redisConnectionFactory, "time-query-lock");
    }

    @Bean
    public Queue timeHttpQueryQueue() {
        return QueueBuilder.durable("time-http-query").build();
    }

    @Bean
    public Queue timePollQueryQueue() {
        return QueueBuilder.durable("time-poll-query").build();
    }

    @Bean
    public Queue timeNotifyQueue() {
        return QueueBuilder.durable("time-notify").build();
    }

    @Bean
    public FanoutExchange fanoutExchange() {
        return new FanoutExchange("amq.fanout");
    }

    @Bean
    public TopicExchange topicExchange() {
        return new TopicExchange("amq.topic");
    }

    @Bean
    public Binding timePollQueryQueueBinding() {
        return BindingBuilder.bind(timePollQueryQueue()).to(topicExchange()).with(timePollQueryQueue().getName());
    }

    @Bean
    public MessageGroupStoreReaper messageGroupStoreReaper(MessageGroupStore messageGroupStore) {
        MessageGroupStoreReaper reaper = new MessageGroupStoreReaper(messageGroupStore);
        reaper.setTimeout(60000 * 10);
        return reaper;
    }

    @Override
    public void setBeanClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }
}
