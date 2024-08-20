package com.zeroclue.jmeter.protocol.amqp;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import org.apache.commons.lang3.StringUtils;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.Interruptible;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testelement.property.TestElementProperty;
import org.apache.logging.log4j.core.lookup.StrSubstitutor;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * JMeter creates an instance of a sampler class for every occurrence of the
 * element in every thread. Some additional copies may be created before the
 * test run starts.
 *
 * <p>Thus each sampler is guaranteed to be called by a single thread - there is no
 * need to synchronize access to instance variables.
 *
 * <p>However, access to class fields must be synchronized.
 */
public class AMQPPublisher extends AMQPSampler implements Interruptible {

    private static final long serialVersionUID = -8420658040465788497L;

    private static final Logger log = LoggerFactory.getLogger(AMQPPublisher.class);

    //++ These are JMX names, and must not be changed
    private static final String MESSAGE = "AMQPPublisher.Message";
    private static final String MESSAGE_ROUTING_KEY = "AMQPPublisher.MessageRoutingKey";
    private static final String MESSAGE_TYPE = "AMQPPublisher.MessageType";
    private static final String REPLY_TO_QUEUE = "AMQPPublisher.ReplyToQueue";
    private static final String CONTENT_TYPE = "AMQPPublisher.ContentType";
    private static final String CORRELATION_ID = "AMQPPublisher.CorrelationId";
    private static final String CONTENT_ENCODING = "AMQPPublisher.ContentEncoding";
    private static final String MESSAGE_ID = "AMQPPublisher.MessageId";
    private static final String MESSAGE_PRIORITY = "AMQPPublisher.MessagePriority";
    private static final String HEADERS = "AMQPPublisher.Headers";
    private static final String PERSISTENT = "AMQPPublisher.Persistent";
    private static final String USE_TX = "AMQPPublisher.UseTx";
    private static final String APP_ID = "AMQPPublisher.AppId";
    private static final String TIMESTAMP = "AMQPPublisher.Timestamp";
    private static final String WAIT_RESPONSE = "AMQPPublisher.WaitResponse";

    public static final boolean DEFAULT_PERSISTENT = false;
    public static final boolean DEFAULT_USE_TX = false;
    public static final boolean DEFAULT_USE_RPC = false;
    public static final boolean DEFAULT_TIMESTAMP = true;
    public static final int DEFAULT_MESSAGE_PRIORITY = 0;
    public static final String DEFAULT_RESPONSE_CODE = "500";
    public static final String DEFAULT_CONTENT_TYPE = "text/plain";
    public static final String DEFAULT_ENCODING = "utf-8";

    private final String replyQueueName = UUID.randomUUID().toString().replace("-", "");

    public AMQPPublisher() {
        super();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SampleResult sample(Entry e) {
        SampleResult result = new SampleResult();
        result.setSampleLabel(getName());
        result.setSuccessful(false);
        result.setResponseCode(DEFAULT_RESPONSE_CODE);

        try {
            initChannel();

            if (getUseRPC()) {
                try (BorrowedChannel channel = borrowChannel()) {
                    channel.getChannel().queueDeclare(replyQueueName, false, false, true, new HashMap<>());
                }

            }
        } catch (Exception ex) {
            //ignore
        }

        String message = getMessage();
        List<String> request = new ArrayList<>();
        List<String> responseData = new ArrayList<>();

        /*
         * Perform the sampling
         */

        // aggregate samples
        int loop = getIterationsAsInt();
        result.sampleStart();   // start timing

        try (BorrowedChannel borrowedChannel = borrowChannel()) {

            Channel channel = borrowedChannel.getChannel();
            if (getUseTx()) {
                channel.txSelect();
            }

            String[] waitCorrelationId = new String[1];

            if (getUseRPC()) {
                final BlockingQueue<String> response = new ArrayBlockingQueue<>(1);

                //try (BorrowedChannel channel2 = borrowChannel()) {
                Channel consumerChannel = channel;// channel2.getChannel();
                String ctag = consumerChannel.basicConsume(replyQueueName, true, (consumerTag, delivery) -> {
                    if (delivery.getProperties().getCorrelationId().equals(waitCorrelationId[0])) {
                        response.offer(new String(delivery.getBody(), getContentEncoding()));
                    }
                }, consumerTag -> {
                });

                for (int idx = 0; idx < loop; idx++) {

                    MessageData messageData = getCreateMessage(waitCorrelationId, message, result, request);
                    request.add(messageData.data);
                    // try to force jms semantics.
                    // but this does not work since RabbitMQ does not sync to disk if consumers are connected as
                    // seen by iostat -cd 1. TPS value remains at 0.

                    channel.basicPublish(getExchange(), getMessageRoutingKey(), messageData.messageProperties, messageData.messageBytes);
                    String responseText = response.poll(getTimeoutAsInt(), TimeUnit.MILLISECONDS);
                    responseData.add(responseText);
                }
                if (ctag != null) {
                    consumerChannel.basicCancel(ctag);
                }
                //}
            } else {

                for (int idx = 0; idx < loop; idx++) {
                    MessageData messageData = getCreateMessage(waitCorrelationId, message, result, request);
                    // try to force jms semantics.
                    // but this does not work since RabbitMQ does not sync to disk if consumers are connected as
                    // seen by iostat -cd 1. TPS value remains at 0.

                    channel.basicPublish(getExchange(), getMessageRoutingKey(), messageData.messageProperties, messageData.messageBytes);
                }
            }

            // commit the sample
            if (getUseTx()) {
                channel.txCommit();
            }

            /*
             * Set up the sample result details
             */

            result.setDataType(SampleResult.TEXT);
            result.setRequestHeaders(formatHeaders());
            result.setSamplerData(StringUtils.join(request,"\n"));
            result.setResponseData(StringUtils.join(responseData, "\n"), StandardCharsets.UTF_8.name());
            result.setResponseCodeOK();
            result.setResponseMessage("OK");
            result.setSuccessful(true);
        } catch (Exception ex) {
            log.warn(ex.getMessage(), ex);
            result.setSamplerData(StringUtils.join(request,"\n"));
            result.setResponseData(StringUtils.join(responseData, "\n"), StandardCharsets.UTF_8.name());
            result.setResponseCode("000");
            result.setResponseMessage(ex.toString());
        } finally {
            result.setSamplerData(StringUtils.join(request,"\n"));
            result.sampleEnd();     // end timing
        }

        return result;
    }

    @NotNull
    private MessageData getCreateMessage(String[] waitCorrelationId, String message, SampleResult result, List<String> request) {
        String correlationId = getCorrelationId();
        if (StringUtils.isEmpty(correlationId)) {
            correlationId = UUID.randomUUID().toString().replace("-", "");
        }
        waitCorrelationId[0] = correlationId;
        AMQP.BasicProperties messageProperties = getProperties(correlationId);
        Map<String, String> props = new HashMap<>();
        props.put("exchange", getExchange());
        props.put("replyTo", getUseRPC() ? replyQueueName : getReplyToQueue());
        props.put("correlationId",correlationId);
        props.put("contentType", getContentType());
        props.put("routingKey", getMessageRoutingKey());
        props.put("contentEncoding", getContentEncoding());
        props.put("messageId", getMessageId());
        props.put("messagePriority", getMessagePriority());
        props.put("type", getMessageType());
        String data = StrSubstitutor.replace(message, props);

        byte[] messageBytes = data.getBytes(StandardCharsets.UTF_8);
        MessageData messageData = new MessageData(messageProperties, data, messageBytes);
        return messageData;
    }

    private static class MessageData {
        public final AMQP.BasicProperties messageProperties;
        public final String data;
        public final byte[] messageBytes;

        public MessageData(AMQP.BasicProperties messageProperties, String data, byte[] messageBytes) {
            this.messageProperties = messageProperties;
            this.data = data;
            this.messageBytes = messageBytes;
        }
    }


    /**
     * @return the message routing key for the sample
     */
    public String getMessageRoutingKey() {
        return getPropertyAsString(MESSAGE_ROUTING_KEY);
    }

    public void setMessageRoutingKey(String content) {
        setProperty(MESSAGE_ROUTING_KEY, content);
    }

    /**
     * @return the message for the sample
     */
    public String getMessage() {
        return getPropertyAsString(MESSAGE);
    }

    public void setMessage(String content) {
        setProperty(MESSAGE, content);
    }

    /**
     * @return the message type for the sample
     */
    public String getMessageType() {
        return getPropertyAsString(MESSAGE_TYPE);
    }

    public void setMessageType(String content) {
        setProperty(MESSAGE_TYPE, content);
    }

    /**
     * @return the reply-to queue for the sample
     */
    public String getReplyToQueue() {
        return getPropertyAsString(REPLY_TO_QUEUE);
    }

    public void setReplyToQueue(String content) {
        setProperty(REPLY_TO_QUEUE, content);
    }

    public String getContentType() {
        return getPropertyAsString(CONTENT_TYPE);
    }

    public void setContentType(String contentType) {
        setProperty(CONTENT_TYPE, contentType);
    }

    public String getContentEncoding() {
        return getPropertyAsString(CONTENT_ENCODING);
    }

    public void setContentEncoding(String contentEncoding) {
        setProperty(CONTENT_ENCODING, contentEncoding);
    }

    /**
     * @return the correlation identifier for the sample
     */
    public String getCorrelationId() {
        return getPropertyAsString(CORRELATION_ID);
    }

    public void setCorrelationId(String content) {
        setProperty(CORRELATION_ID, content);
    }

    /**
     * @return the message id for the sample
     */
    public String getMessageId() {
        return getPropertyAsString(MESSAGE_ID);
    }

    public void setMessageId(String content) {
        setProperty(MESSAGE_ID, content);
    }

    /**
     * @return the message priority for the sample
     */
    public String getMessagePriority() {
        return getPropertyAsString(MESSAGE_PRIORITY);
    }

    public void setMessagePriority(String content) {
        setProperty(MESSAGE_PRIORITY, content);
    }

    protected int getMessagePriorityAsInt() {
        if (getPropertyAsInt(MESSAGE_PRIORITY) < 0) {
            return 0;
        }

        return getPropertyAsInt(MESSAGE_PRIORITY);
    }

    public Arguments getHeaders() {
        return (Arguments) getProperty(HEADERS).getObjectValue();
    }

    public void setHeaders(Arguments headers) {
        setProperty(new TestElementProperty(HEADERS, headers));
    }

    public boolean getPersistent() {
        return getPropertyAsBoolean(PERSISTENT, DEFAULT_PERSISTENT);
    }

    public void setPersistent(Boolean persistent) {
        setProperty(PERSISTENT, persistent);
    }

    public boolean getUseTx() {
        return getPropertyAsBoolean(USE_TX, DEFAULT_USE_TX);
    }

    public void setUseTx(Boolean tx) {
        setProperty(USE_TX, tx);
    }

    public String getAppId() {
        return getPropertyAsString(APP_ID);
    }

    public void setAppId(String appId) {
        setProperty(APP_ID, appId);
    }

    public boolean getTimestamp() {
        return getPropertyAsBoolean(TIMESTAMP, DEFAULT_TIMESTAMP);
    }

    public void setTimestamp(Boolean ts) {
        setProperty(TIMESTAMP, ts);
    }

    public boolean getUseRPC() {
        return getPropertyAsBoolean(WAIT_RESPONSE, DEFAULT_USE_RPC);
    }

    public void setUseRPC(Boolean waitResponse) {
        setProperty(WAIT_RESPONSE, waitResponse);
    }

    @Override
    public boolean interrupt() {
        cleanup();
        return true;
    }


    protected AMQP.BasicProperties getProperties(String correlationId) {
        final AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();
        final int deliveryMode = getPersistent() ? 2 : 1;
        final String contentType = StringUtils.defaultIfEmpty(getContentType(), DEFAULT_CONTENT_TYPE);

        builder.contentType(contentType)
            .contentEncoding(getContentEncoding())
            .deliveryMode(deliveryMode)
            .correlationId(correlationId)
            .replyTo(getUseRPC() ? replyQueueName : getReplyToQueue())
            .type(getMessageType())
            .headers(prepareHeaders());

        if (getMessageId() != null && !getMessageId().isEmpty()) {
            builder.messageId(getMessageId());
        }

        if (getMessagePriority() != null && !getMessagePriority().isEmpty()) {
            builder.priority(getMessagePriorityAsInt());
        } else {
            builder.priority(DEFAULT_MESSAGE_PRIORITY);
        }

        if (getAppId() != null && !getAppId().isEmpty()) {
            builder.appId(getAppId());
        }

        if (getTimestamp()) {
            builder.timestamp(Date.from(Instant.now()));
        }

        return builder.build();
    }


    private Map<String, Object> prepareHeaders() {
        Arguments headers = getHeaders();

        if (headers != null) {
            return new HashMap<>(headers.getArgumentsAsMap());
        }

        return Collections.emptyMap();
    }

    private String formatHeaders() {
        Map<String, String> headers = getHeaders().getArgumentsAsMap();
        StringBuilder sb = new StringBuilder();

        for (Map.Entry<String, String> entry : headers.entrySet()) {
            sb.append(entry.getKey())
                .append(": ")
                .append(entry.getValue())
                .append("\n");
        }

        return sb.toString();
    }
}
