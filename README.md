# Spring Integration Aggregation Example

After starting up the application (`gradle bootRun`), you can trigger the flow by calling `src/main/bash/start.sh` provided you have `curl` installed.


## Serialization Error

I do not have serialization issues by using the `SimpleMessageStore`. However, I see the following if I use a Redis `MessageStoreGroup`:

```
2017-10-03 17:44:13.337  WARN 25078 --- [erContainer#0-1] s.a.r.l.ConditionalRejectingErrorHandler : Execution of Rabbit message listener failed.

org.springframework.amqp.rabbit.listener.exception.ListenerExecutionFailedException: Listener threw exception
	at org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer.wrapToListenerExecutionFailedExceptionIfNeeded(AbstractMessageListenerContainer.java:941) ~[spring-rabbit-1.7.4.RELEASE.jar:na]
	at org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer.doInvokeListener(AbstractMessageListenerContainer.java:851) ~[spring-rabbit-1.7.4.RELEASE.jar:na]
	at org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer.invokeListener(AbstractMessageListenerContainer.java:771) ~[spring-rabbit-1.7.4.RELEASE.jar:na]
	at org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer.access$001(SimpleMessageListenerContainer.java:102) [spring-rabbit-1.7.4.RELEASE.jar:na]
	at org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer$1.invokeListener(SimpleMessageListenerContainer.java:198) ~[spring-rabbit-1.7.4.RELEASE.jar:na]
	at org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer.invokeListener(SimpleMessageListenerContainer.java:1311) [spring-rabbit-1.7.4.RELEASE.jar:na]
	at org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer.executeListener(AbstractMessageListenerContainer.java:752) ~[spring-rabbit-1.7.4.RELEASE.jar:na]
	at org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer.doReceiveAndExecute(SimpleMessageListenerContainer.java:1254) [spring-rabbit-1.7.4.RELEASE.jar:na]
	at org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer.receiveAndExecute(SimpleMessageListenerContainer.java:1224) [spring-rabbit-1.7.4.RELEASE.jar:na]
	at org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer.access$1600(SimpleMessageListenerContainer.java:102) [spring-rabbit-1.7.4.RELEASE.jar:na]
	at org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer$AsyncMessageProcessingConsumer.run(SimpleMessageListenerContainer.java:1470) [spring-rabbit-1.7.4.RELEASE.jar:na]
	at java.lang.Thread.run(Thread.java:748) [na:1.8.0_131]
Caused by: org.springframework.integration.transformer.MessageTransformationException: Failed to transform Message; nested exception is org.springframework.messaging.MessageHandlingException: nested exception is java.lang.ClassCastException: org.sdf.rkm.aggex.domain.TimeResponse cannot be cast to org.sdf.rkm.aggex.domain.TimeResponse, failedMessage=GenericMessage [payload=[org.sdf.rkm.aggex.domain.TimeResponse@4d6e12a0, org.sdf.rkm.aggex.domain.TimeResponse@31f1ff3c, org.sdf.rkm.aggex.domain.TimeResponse@3ea5a993, org.sdf.rkm.aggex.domain.TimeResponse@10f0c23e, org.sdf.rkm.aggex.domain.TimeResponse@2d363992, org.sdf.rkm.aggex.domain.TimeResponse@13003b17, org.sdf.rkm.aggex.domain.TimeResponse@7af1d5, org.sdf.rkm.aggex.domain.TimeResponse@168d22d6, org.sdf.rkm.aggex.domain.TimeResponse@a6b733c, org.sdf.rkm.aggex.domain.TimeResponse@182129e5, org.sdf.rkm.aggex.domain.TimeResponse@4f02976d, org.sdf.rkm.aggex.domain.TimeResponse@30e697c9, org.sdf.rkm.aggex.domain.TimeResponse@6145f07a, org.sdf.rkm.aggex.domain.TimeResponse@70ba93b6, org.sdf.rkm.aggex.domain.TimeResponse@2b58cb33, org.sdf.rkm.aggex.domain.TimeResponse@2f04594e, org.sdf.rkm.aggex.domain.TimeResponse@618caaf4, org.sdf.rkm.aggex.domain.TimeResponse@458fb250, org.sdf.rkm.aggex.domain.TimeResponse@28e3f881, org.sdf.rkm.aggex.domain.TimeResponse@25a00afa, org.sdf.rkm.aggex.domain.TimeResponse@21ffdfb0, org.sdf.rkm.aggex.domain.TimeResponse@195ae4ed, org.sdf.rkm.aggex.domain.TimeResponse@2eca30b7, org.sdf.rkm.aggex.domain.TimeResponse@b18084e], headers={amqp_receivedDeliveryMode=PERSISTENT, http_requestMethod=GET, sequenceNumber=24, amqp_deliveryTag=24, sequenceSize=24, amqp_consumerQueue=time-query, amqp_redelivered=false, accept=*/*, amqp_receivedRoutingKey=time-query, host=localhost:8080, http_requestUrl=http://localhost:8080/start, correlationId=5a982a42-0917-1e3c-50da-db6845657412, id=063257d9-8a02-6a5d-db7c-eed433813810, amqp_consumerTag=amq.ctag-omX39M6IugTDmBmVVNxuHA, contentType=application/x-java-serialized-object, user-agent=curl/7.47.0, timestamp=1507070653324}]
	at org.springframework.integration.transformer.MessageTransformingHandler.handleRequestMessage(MessageTransformingHandler.java:95) ~[spring-integration-core-4.3.12.RELEASE.jar:4.3.12.RELEASE]
	at org.springframework.integration.handler.AbstractReplyProducingMessageHandler.handleMessageInternal(AbstractReplyProducingMessageHandler.java:109) ~[spring-integration-core-4.3.12.RELEASE.jar:4.3.12.RELEASE]
	at org.springframework.integration.handler.AbstractMessageHandler.handleMessage(AbstractMessageHandler.java:127) ~[spring-integration-core-4.3.12.RELEASE.jar:4.3.12.RELEASE]
	at org.springframework.integration.dispatcher.AbstractDispatcher.tryOptimizedDispatch(AbstractDispatcher.java:116) ~[spring-integration-core-4.3.12.RELEASE.jar:4.3.12.RELEASE]
	at org.springframework.integration.dispatcher.UnicastingDispatcher.doDispatch(UnicastingDispatcher.java:148) ~[spring-integration-core-4.3.12.RELEASE.jar:4.3.12.RELEASE]
	at org.springframework.integration.dispatcher.UnicastingDispatcher.dispatch(UnicastingDispatcher.java:121) ~[spring-integration-core-4.3.12.RELEASE.jar:4.3.12.RELEASE]
	at org.springframework.integration.channel.AbstractSubscribableChannel.doSend(AbstractSubscribableChannel.java:89) ~[spring-integration-core-4.3.12.RELEASE.jar:4.3.12.RELEASE]
	at org.springframework.integration.channel.AbstractMessageChannel.send(AbstractMessageChannel.java:425) ~[spring-integration-core-4.3.12.RELEASE.jar:4.3.12.RELEASE]
	at org.springframework.integration.channel.AbstractMessageChannel.send(AbstractMessageChannel.java:375) ~[spring-integration-core-4.3.12.RELEASE.jar:4.3.12.RELEASE]
	at org.springframework.messaging.core.GenericMessagingTemplate.doSend(GenericMessagingTemplate.java:115) ~[spring-messaging-4.3.11.RELEASE.jar:4.3.11.RELEASE]
	at org.springframework.messaging.core.GenericMessagingTemplate.doSend(GenericMessagingTemplate.java:45) ~[spring-messaging-4.3.11.RELEASE.jar:4.3.11.RELEASE]
	at org.springframework.messaging.core.AbstractMessageSendingTemplate.send(AbstractMessageSendingTemplate.java:105) ~[spring-messaging-4.3.11.RELEASE.jar:4.3.11.RELEASE]
	at org.springframework.integration.handler.AbstractMessageProducingHandler.sendOutput(AbstractMessageProducingHandler.java:360) ~[spring-integration-core-4.3.12.RELEASE.jar:4.3.12.RELEASE]
	at org.springframework.integration.handler.AbstractMessageProducingHandler.produceOutput(AbstractMessageProducingHandler.java:271) ~[spring-integration-core-4.3.12.RELEASE.jar:4.3.12.RELEASE]
	at org.springframework.integration.handler.AbstractMessageProducingHandler.sendOutputs(AbstractMessageProducingHandler.java:188) ~[spring-integration-core-4.3.12.RELEASE.jar:4.3.12.RELEASE]
	at org.springframework.integration.aggregator.AbstractCorrelatingMessageHandler.completeGroup(AbstractCorrelatingMessageHandler.java:671) ~[spring-integration-core-4.3.12.RELEASE.jar:4.3.12.RELEASE]
	at org.springframework.integration.aggregator.AbstractCorrelatingMessageHandler.handleMessageInternal(AbstractCorrelatingMessageHandler.java:418) ~[spring-integration-core-4.3.12.RELEASE.jar:4.3.12.RELEASE]
	at org.springframework.integration.handler.AbstractMessageHandler.handleMessage(AbstractMessageHandler.java:127) ~[spring-integration-core-4.3.12.RELEASE.jar:4.3.12.RELEASE]
	at org.springframework.integration.dispatcher.AbstractDispatcher.tryOptimizedDispatch(AbstractDispatcher.java:116) ~[spring-integration-core-4.3.12.RELEASE.jar:4.3.12.RELEASE]
	at org.springframework.integration.dispatcher.UnicastingDispatcher.doDispatch(UnicastingDispatcher.java:148) ~[spring-integration-core-4.3.12.RELEASE.jar:4.3.12.RELEASE]
	at org.springframework.integration.dispatcher.UnicastingDispatcher.dispatch(UnicastingDispatcher.java:121) ~[spring-integration-core-4.3.12.RELEASE.jar:4.3.12.RELEASE]
	at org.springframework.integration.channel.AbstractSubscribableChannel.doSend(AbstractSubscribableChannel.java:89) ~[spring-integration-core-4.3.12.RELEASE.jar:4.3.12.RELEASE]
	at org.springframework.integration.channel.AbstractMessageChannel.send(AbstractMessageChannel.java:425) ~[spring-integration-core-4.3.12.RELEASE.jar:4.3.12.RELEASE]
	at org.springframework.integration.channel.AbstractMessageChannel.send(AbstractMessageChannel.java:375) ~[spring-integration-core-4.3.12.RELEASE.jar:4.3.12.RELEASE]
	at org.springframework.messaging.core.GenericMessagingTemplate.doSend(GenericMessagingTemplate.java:115) ~[spring-messaging-4.3.11.RELEASE.jar:4.3.11.RELEASE]
	at org.springframework.messaging.core.GenericMessagingTemplate.doSend(GenericMessagingTemplate.java:45) ~[spring-messaging-4.3.11.RELEASE.jar:4.3.11.RELEASE]
	at org.springframework.messaging.core.AbstractMessageSendingTemplate.send(AbstractMessageSendingTemplate.java:105) ~[spring-messaging-4.3.11.RELEASE.jar:4.3.11.RELEASE]
	at org.springframework.integration.handler.AbstractMessageProducingHandler.sendOutput(AbstractMessageProducingHandler.java:360) ~[spring-integration-core-4.3.12.RELEASE.jar:4.3.12.RELEASE]
	at org.springframework.integration.handler.AbstractMessageProducingHandler.produceOutput(AbstractMessageProducingHandler.java:271) ~[spring-integration-core-4.3.12.RELEASE.jar:4.3.12.RELEASE]
	at org.springframework.integration.handler.AbstractMessageProducingHandler.sendOutputs(AbstractMessageProducingHandler.java:188) ~[spring-integration-core-4.3.12.RELEASE.jar:4.3.12.RELEASE]
	at org.springframework.integration.handler.AbstractReplyProducingMessageHandler.handleMessageInternal(AbstractReplyProducingMessageHandler.java:115) ~[spring-integration-core-4.3.12.RELEASE.jar:4.3.12.RELEASE]
	at org.springframework.integration.handler.AbstractMessageHandler.handleMessage(AbstractMessageHandler.java:127) ~[spring-integration-core-4.3.12.RELEASE.jar:4.3.12.RELEASE]
	at org.springframework.integration.dispatcher.AbstractDispatcher.tryOptimizedDispatch(AbstractDispatcher.java:116) ~[spring-integration-core-4.3.12.RELEASE.jar:4.3.12.RELEASE]
	at org.springframework.integration.dispatcher.UnicastingDispatcher.doDispatch(UnicastingDispatcher.java:148) ~[spring-integration-core-4.3.12.RELEASE.jar:4.3.12.RELEASE]
	at org.springframework.integration.dispatcher.UnicastingDispatcher.dispatch(UnicastingDispatcher.java:121) ~[spring-integration-core-4.3.12.RELEASE.jar:4.3.12.RELEASE]
	at org.springframework.integration.channel.AbstractSubscribableChannel.doSend(AbstractSubscribableChannel.java:89) ~[spring-integration-core-4.3.12.RELEASE.jar:4.3.12.RELEASE]
	at org.springframework.integration.channel.AbstractMessageChannel.send(AbstractMessageChannel.java:425) ~[spring-integration-core-4.3.12.RELEASE.jar:4.3.12.RELEASE]
	at org.springframework.integration.channel.AbstractMessageChannel.send(AbstractMessageChannel.java:375) ~[spring-integration-core-4.3.12.RELEASE.jar:4.3.12.RELEASE]
	at org.springframework.messaging.core.GenericMessagingTemplate.doSend(GenericMessagingTemplate.java:115) ~[spring-messaging-4.3.11.RELEASE.jar:4.3.11.RELEASE]
	at org.springframework.messaging.core.GenericMessagingTemplate.doSend(GenericMessagingTemplate.java:45) ~[spring-messaging-4.3.11.RELEASE.jar:4.3.11.RELEASE]
	at org.springframework.messaging.core.AbstractMessageSendingTemplate.send(AbstractMessageSendingTemplate.java:105) ~[spring-messaging-4.3.11.RELEASE.jar:4.3.11.RELEASE]
	at org.springframework.integration.endpoint.MessageProducerSupport.sendMessage(MessageProducerSupport.java:188) ~[spring-integration-core-4.3.12.RELEASE.jar:4.3.12.RELEASE]
	at org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter.access$1100(AmqpInboundChannelAdapter.java:56) ~[spring-integration-amqp-4.3.12.RELEASE.jar:4.3.12.RELEASE]
	at org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter$Listener.processMessage(AmqpInboundChannelAdapter.java:246) ~[spring-integration-amqp-4.3.12.RELEASE.jar:4.3.12.RELEASE]
	at org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter$Listener.onMessage(AmqpInboundChannelAdapter.java:203) ~[spring-integration-amqp-4.3.12.RELEASE.jar:4.3.12.RELEASE]
	at org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer.doInvokeListener(AbstractMessageListenerContainer.java:848) ~[spring-rabbit-1.7.4.RELEASE.jar:na]
	... 10 common frames omitted
Caused by: org.springframework.messaging.MessageHandlingException: nested exception is java.lang.ClassCastException: org.sdf.rkm.aggex.domain.TimeResponse cannot be cast to org.sdf.rkm.aggex.domain.TimeResponse
	at org.springframework.integration.dsl.LambdaMessageProcessor.processMessage(LambdaMessageProcessor.java:130) ~[spring-integration-java-dsl-1.2.3.RELEASE.jar:na]
	at org.springframework.integration.transformer.AbstractMessageProcessingTransformer.transform(AbstractMessageProcessingTransformer.java:90) ~[spring-integration-core-4.3.12.RELEASE.jar:4.3.12.RELEASE]
	at org.springframework.integration.transformer.MessageTransformingHandler.handleRequestMessage(MessageTransformingHandler.java:89) ~[spring-integration-core-4.3.12.RELEASE.jar:4.3.12.RELEASE]
	... 55 common frames omitted
Caused by: java.lang.ClassCastException: org.sdf.rkm.aggex.domain.TimeResponse cannot be cast to org.sdf.rkm.aggex.domain.TimeResponse
	at java.util.stream.ReferencePipeline$3$1.accept(ReferencePipeline.java:193) ~[na:1.8.0_131]
	at java.util.ArrayList$ArrayListSpliterator.forEachRemaining(ArrayList.java:1374) ~[na:1.8.0_131]
	at java.util.stream.AbstractPipeline.copyInto(AbstractPipeline.java:481) ~[na:1.8.0_131]
	at java.util.stream.AbstractPipeline.wrapAndCopyInto(AbstractPipeline.java:471) ~[na:1.8.0_131]
	at java.util.stream.ReduceOps$ReduceOp.evaluateSequential(ReduceOps.java:708) ~[na:1.8.0_131]
	at java.util.stream.AbstractPipeline.evaluate(AbstractPipeline.java:234) ~[na:1.8.0_131]
	at java.util.stream.ReferencePipeline.collect(ReferencePipeline.java:499) ~[na:1.8.0_131]
	at org.sdf.rkm.aggex.config.PipelineConfig.lambda$timeRequestFlow$5(PipelineConfig.java:62) ~[main/:na]
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method) ~[na:1.8.0_131]
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62) ~[na:1.8.0_131]
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) ~[na:1.8.0_131]
	at java.lang.reflect.Method.invoke(Method.java:498) ~[na:1.8.0_131]
	at org.springframework.integration.dsl.LambdaMessageProcessor.processMessage(LambdaMessageProcessor.java:127) ~[spring-integration-java-dsl-1.2.3.RELEASE.jar:na]
	... 57 common frames omitted
```