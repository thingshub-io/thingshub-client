package io.thingshub.client;

import static io.thingshub.commons.model.MessageType.SERVICE_FUNCTION_CALL;
import static io.thingshub.commons.model.MessageType.SERVICE_REQUEST_REPLY;
import static io.thingshub.commons.model.ThingshubConstants.SERVICE_CLIENT_INTERNAL_MESSAGE_CHANGE_MESSAGE_AUTHORIZATION;
import static io.thingshub.commons.model.ThingshubConstants.SERVICE_CLIENT_INTERNAL_MESSAGE_CHANGE_PRODUCT_BINDING;
import static io.thingshub.commons.model.ThingshubConstants.SERVICE_CLIENT_INTERNAL_MESSAGE_DEVICE_INFO;
import static io.thingshub.commons.model.ThingshubConstants.SERVICE_CLIENT_INTERNAL_MESSAGE_QUERY_DEVICE;
import static io.thingshub.commons.model.ThingshubConstants.SERVICE_CLIENT_INTERNAL_MESSAGE_QUERY_MESSAGE_DEFINITION;
import static io.thingshub.commons.model.ThingshubConstants.SERVICE_CLIENT_INTERNAL_MESSAGE_QUERY_PRODUCT_BINDING;
import static io.thingshub.commons.model.ThingshubConstants.SERVICE_CLIENT_INTERNAL_TOPIC_CHANGE_MESSAGE_AUTHORIZATION;
import static io.thingshub.commons.model.ThingshubConstants.SERVICE_CLIENT_INTERNAL_TOPIC_CHANGE_PRODUCT_BINDING;
import static io.thingshub.commons.model.ThingshubConstants.SERVICE_CLIENT_INTERNAL_TOPIC_DEVICE_INFO;
import static io.thingshub.commons.model.ThingshubConstants.SERVICE_CLIENT_INTERNAL_TOPIC_DEVICE_INFO_REPLY;
import static io.thingshub.commons.model.ThingshubConstants.SERVICE_CLIENT_INTERNAL_TOPIC_QUERY_DEVICE;
import static io.thingshub.commons.model.ThingshubConstants.SERVICE_CLIENT_INTERNAL_TOPIC_QUERY_DEVICE_REPLY;
import static io.thingshub.commons.model.ThingshubConstants.SERVICE_CLIENT_INTERNAL_TOPIC_QUERY_MESSAGE_DEFINITION;
import static io.thingshub.commons.model.ThingshubConstants.SERVICE_CLIENT_INTERNAL_TOPIC_QUERY_MESSAGE_DEFINITION_REPLY;
import static io.thingshub.commons.model.ThingshubConstants.SERVICE_CLIENT_INTERNAL_TOPIC_QUERY_PRODUCT_BINDING;
import static io.thingshub.commons.model.ThingshubConstants.SERVICE_CLIENT_INTERNAL_TOPIC_QUERY_PRODUCT_BINDING_REPLY;
import static io.thingshub.commons.model.ThingshubConstants.THING_TOPIC_EVENT_POST;
import static io.thingshub.commons.model.ThingshubConstants.THING_TOPIC_EVENT_REPLY;
import static io.thingshub.commons.model.ThingshubConstants.THING_TOPIC_PROPERTY_POST;
import static io.thingshub.commons.model.ThingshubConstants.THING_TOPIC_PROPERTY_REPLY;
import static io.thingshub.commons.model.ThingshubConstants.THING_TOPIC_SERVICE_FUNCTION_CALL;
import static io.thingshub.commons.model.ThingshubConstants.THING_TOPIC_SERVICE_FUNCTION_REPLY;
import static io.thingshub.commons.model.ThingshubConstants.THING_TOPIC_SERVICE_REQUEST_POST;
import static io.thingshub.commons.model.ThingshubConstants.THING_TOPIC_SERVICE_REQUEST_REPLY;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.eclipse.paho.mqttv5.client.MqttCallback;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.google.common.base.Strings;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

import io.thingshub.commons.meta.ValidateResult;
import io.thingshub.commons.meta.types.DataType;
import io.thingshub.commons.model.MessageParameter;
import io.thingshub.commons.model.MessageResult;
import io.thingshub.commons.model.MessageSpec;
import io.thingshub.commons.model.MessageType;
import io.thingshub.commons.model.Page;
import io.thingshub.commons.model.ThingshubMessage;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class ThingshubClient {

	private static final String MESSAGE_MODEL_VERSION = "1.0.0";

	private static final String PREFIX_SHARE_TOPIC = "$share/";

	private final Set<String> product_codes = Sets.newConcurrentHashSet();
	private final Table<String, String, MessageSpec> message_spec_table = HashBasedTable.create();

	private final Map<String, ReplyHandler<?>> pending_reply_handlers = Maps.newConcurrentMap();
	private final Map<String, ScheduledFuture<?>> reply_timeout_tasks = Maps.newConcurrentMap();

	private final ScheduledExecutorService timeoutScheduler = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors() + 1);
	private final ExecutorService replyExecutor = Executors.newCachedThreadPool(r -> new Thread(r, "thingshub-reply-handler-" + UUID.randomUUID().toString().substring(0, 8)));

	private final Map<String, MessageProcessor<?>> message_processors = new HashMap<>();
	private final Map<String, Type> message_types = new HashMap<>();

	private int defaultTimeout = 30;// seconds

	private CountDownLatch initLatch = new CountDownLatch(1000);

	private String topicShareGroup;

	private MqttAsyncClient mqttAsyncClient;

	@Value("${thingshub.host}")
	private String host;

	@Value("${thingshub.port: 1883}")
	private Integer port;

	@Value("${thingshub.username}")
	private String username;

	@Value("${thingshub.password}")
	private String password;

	@Value("${thingshub.client-id:}")
	private String clientId;

	@Value("${thingshub.clean-start: true}")
	private Boolean cleanStart;

	@Value("${thingshub.connect-timeout: 30}")
	private Integer connectTimeout = 30;

	@Value("${thingshub.max-reconnect-delay: 128}")
	private Integer maxReconnectDelay = 128;

	@Value("${thingshub.keep-alive: 60}")
	private Integer keepAlive = 60;

	@PostConstruct
	private void init() {
		if (Strings.isNullOrEmpty(host)) {
			throw new ThingshubException("thigshub host is required");
		}
		if (Objects.isNull(port)) {
			throw new ThingshubException("thingshub port is required");
		}
		if (Strings.isNullOrEmpty(username)) {
			throw new ThingshubException("thingshub user name is required");
		}
		if (Strings.isNullOrEmpty(password)) {
			throw new ThingshubException("thingshub password is required");
		}

		if (Strings.isNullOrEmpty(clientId)) {
			clientId = "thingshub-client-" + ThreadLocalRandom.current().nextInt(10000, 100000);
			log.info("no client id specified for this thingshub client, and a random client id [{}] is assigned", clientId);
		}

		this.topicShareGroup = PREFIX_SHARE_TOPIC + username + "/";

		start();
	}

	private void start() {
		MqttConnectionOptions opts = new MqttConnectionOptions();
		opts.setUserName(username);
		opts.setPassword(Optional.ofNullable(password).orElseGet(() -> "").getBytes(StandardCharsets.UTF_8));
		opts.setCleanStart(cleanStart);
		opts.setConnectionTimeout(connectTimeout.intValue());
		opts.setMaxReconnectDelay(maxReconnectDelay.intValue() * 1000);
		opts.setKeepAliveInterval(keepAlive.intValue());
		opts.setAutomaticReconnect(true);

		try {
			String serverUri = String.format("tcp://%s:%d", host, port);
			mqttAsyncClient = new MqttAsyncClient(serverUri, clientId, new MemoryPersistence());
			mqttAsyncClient.setCallback(new MqttCallback() {

				@Override
				public void disconnected(MqttDisconnectResponse disconnectResponse) {
					log.warn("{} disconnected from thingshub server. code: {}, reason: {}", clientId, disconnectResponse.getReturnCode(), disconnectResponse.getReasonString());

					if (disconnectResponse.getException() != null) {
						log.error("", disconnectResponse.getException());
					}
				}

				@Override
				public void mqttErrorOccurred(MqttException exception) {
					log.error("mqtt error: ", exception);
				}

				@Override
				public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
					if (log.isDebugEnabled()) {
						log.debug("thingshub message arrived, topic: {}, payload: {}", topic, new String(mqttMessage.getPayload()));
					}

					ThingshubMessage thingshubMessage = JSON.parseObject(new String(mqttMessage.getPayload()), ThingshubMessage.class);
					if (thingshubMessage.getName().equals(SERVICE_CLIENT_INTERNAL_MESSAGE_CHANGE_PRODUCT_BINDING)) {
						JSONObject params = (JSONObject) thingshubMessage.getParams();
						String action = params.getString("action");
						List<String> productCodes = params.getList("productCodes", String.class);

						refreshProducts(action, productCodes);
					} else if (thingshubMessage.getName().equals(SERVICE_CLIENT_INTERNAL_MESSAGE_CHANGE_MESSAGE_AUTHORIZATION)) {
						JSONObject params = (JSONObject) thingshubMessage.getParams();
						String productCode = params.getString("productCode");
						if (product_codes.contains(productCode)) {
							String action = params.getString("action");
							String messageName = params.getString("messageName");
							MessageSpec messageSpec = params.getObject("messageSpec", MessageSpec.class);
							refreshMessageSpec(action, messageName, messageSpec);
						}
					} else if (thingshubMessage.getCode() != null) {// device reply message
						ScheduledFuture<?> replyTimeoutTask = reply_timeout_tasks.remove(thingshubMessage.getId());
						if (replyTimeoutTask != null) {
							replyTimeoutTask.cancel(false);
						}
						ReplyHandler<?> replyHandler = pending_reply_handlers.remove(thingshubMessage.getId());
						if (replyHandler != null) {
							handleReply(thingshubMessage, replyHandler);
						} else {
							processReplyMessage(thingshubMessage.getClientId(), thingshubMessage.getName(), thingshubMessage.getId(), thingshubMessage.getCode(),
									thingshubMessage.getMessage(), thingshubMessage.getData());
						}
					} else {// device publish message
						processPublishMessage(thingshubMessage.getClientId(), thingshubMessage.getName(), thingshubMessage.getId(), thingshubMessage.getParams());
					}
				}

				@Override
				public void deliveryComplete(IMqttToken token) {
					try {
						if (token.getMessage() != null) {
							log.debug("message delivered, payload: {}, qos: {}", new String(token.getMessage().getPayload()), token.getMessage().getQos());
						} else {
							log.debug("message delivered");
						}
					} catch (MqttException e) {
						log.error("", e);
					}
				}

				@Override
				public void connectComplete(boolean reconnect, String serverURI) {
					log.info("{} has connected to thingshub mqtt server", clientId);

					subscribe(String.format(SERVICE_CLIENT_INTERNAL_TOPIC_CHANGE_PRODUCT_BINDING, username, "+"), 2);
					subscribe(String.format(SERVICE_CLIENT_INTERNAL_TOPIC_CHANGE_MESSAGE_AUTHORIZATION, username, "+"), 2);
					subscribe(String.format(SERVICE_CLIENT_INTERNAL_TOPIC_QUERY_PRODUCT_BINDING_REPLY, username, clientId), 2);
					subscribe(String.format(SERVICE_CLIENT_INTERNAL_TOPIC_QUERY_MESSAGE_DEFINITION_REPLY, username, clientId), 2);
					subscribe(String.format(SERVICE_CLIENT_INTERNAL_TOPIC_QUERY_DEVICE_REPLY, username, clientId), 2);
					subscribe(String.format(SERVICE_CLIENT_INTERNAL_TOPIC_DEVICE_INFO_REPLY, username, clientId), 2);

					queryProductBindings();
				}

				@Override
				public void authPacketArrived(int reasonCode, MqttProperties properties) {

				}

			});
		} catch (MqttException e) {
			log.error("", e);

			throw new ThingshubException("create MqttAsyncClient error: " + e.getMessage());
		}

		if (mqttAsyncClient != null) {
			while (!mqttAsyncClient.isConnected()) {
				try {
					IMqttToken token = mqttAsyncClient.connect(opts);
					token.waitForCompletion();
				} catch (Exception e) {
					log.error("", e);

					throw new ThingshubException("connect error: " + e.getMessage());
				}
			}
		}
	}

	private void processPublishMessage(String sn, String messageName, String messageId, Object params) {
		MessageProcessor<?> theMessageProcessor = message_processors.get(messageName);
		if (theMessageProcessor == null) {
			synchronized (this) {
				theMessageProcessor = message_processors.get(messageName);

				if (theMessageProcessor == null) {
					@SuppressWarnings("rawtypes")
					Map<String, MessageProcessor> messageProcessors = SpringUtils.getBeans(MessageProcessor.class);
					if (messageProcessors != null) {
						messageProcessors.values().forEach(mp -> {
							message_processors.put(mp.getMessageName(), mp);

							Type[] types = ((ParameterizedType) mp.getClass().getGenericInterfaces()[0]).getActualTypeArguments();
							message_types.put(mp.getMessageName(), types[0]);
						});

						theMessageProcessor = message_processors.get(messageName);
					}
				}
			}
		}

		if (theMessageProcessor != null) {
			if (params != null) {
				Type type = message_types.get(messageName);
				theMessageProcessor.process(sn, messageId, JSON.parseObject(JSON.toJSONString(params), type));
			} else {
				theMessageProcessor.process(sn, messageId, null);
			}
		} else {
			log.warn("no {} message processor is defined. sn: {}, message id: {}, message content: {}", messageName, sn, messageId, JSON.toJSONString(params));
		}
	}

	private void processReplyMessage(String sn, String messageName, String messageId, Integer code, String error, Object data) {
		MessageProcessor<?> theMessageProcessor = message_processors.get(messageName);
		if (theMessageProcessor == null) {
			synchronized (this) {
				theMessageProcessor = message_processors.get(messageName);

				if (theMessageProcessor == null) {
					@SuppressWarnings("rawtypes")
					Map<String, MessageProcessor> messageProcessors = SpringUtils.getBeans(MessageProcessor.class);
					if (messageProcessors != null) {
						messageProcessors.values().forEach(mp -> {
							message_processors.put(mp.getMessageName(), mp);

							Type[] types = ((ParameterizedType) mp.getClass().getGenericInterfaces()[0]).getActualTypeArguments();
							message_types.put(mp.getMessageName(), types[0]);
						});

						theMessageProcessor = message_processors.get(messageName);
					}
				}
			}
		}

		if (theMessageProcessor != null) {
			if (code.intValue() == MessageResult.SUCCESS.code()) {
				if (data != null) {
					Type type = message_types.get(messageName);
					theMessageProcessor.process(sn, messageId, JSON.parseObject(JSON.toJSONString(data), type));
				} else {
					theMessageProcessor.process(sn, messageId, null);
				}
			} else {
				theMessageProcessor.onError(sn, code, error);
			}
		} else {
			log.warn("no {} message processor is defined. sn: {}, message id: {}, message content: {}", messageName, sn, messageId, JSON.toJSONString(data));
		}
	}

	private <T> void handleReply(ThingshubMessage thingshubMessage, ReplyHandler<T> replyHandler) {
		replyExecutor.submit(() -> {
			try {
				if (thingshubMessage.getCode().intValue() == MessageResult.SUCCESS.code()) {
					if (thingshubMessage.getData() != null) {
						String dataStr = JSON.toJSONString(thingshubMessage.getData());
						Type[] types = ((ParameterizedType) replyHandler.getClass().getGenericInterfaces()[0]).getActualTypeArguments();
						T dataObj = JSON.parseObject(dataStr, types[0]);
						replyHandler.onSuccess(dataObj);
					} else {
						replyHandler.onSuccess(null);
					}
				} else {
					replyHandler.onError(new ThingshubException(thingshubMessage.getMessage()));
				}
			} catch (Exception e) {
				log.error("", e);

				replyHandler.onError(e);
			} finally {
				replyHandler.onComplete();
			}
		});
	}

	private void queryProductBindings() {
		String messageId = UUID.randomUUID().toString();

		pending_reply_handlers.put(messageId, new ReplyHandler<List<String>>() {

			@Override
			public void onSuccess(List<String> data) {
				for (int i = 0; i < 1000 - data.size(); i++) {
					initLatch.countDown();
				}

				for (String productCode : data) {
					product_codes.add(productCode);

					subscribe(topicShareGroup + String.format(THING_TOPIC_PROPERTY_POST, productCode, "+", "+"), 2);
					subscribe(topicShareGroup + String.format(THING_TOPIC_SERVICE_FUNCTION_REPLY, productCode, "+", "+"), 2);
					subscribe(topicShareGroup + String.format(THING_TOPIC_SERVICE_REQUEST_POST, productCode, "+", "+"), 2);
					subscribe(topicShareGroup + String.format(THING_TOPIC_EVENT_POST, productCode, "+", "+"), 2);

					queryMessageSpecs(productCode);
				}
			}

			@Override
			public void onError(Throwable cause) {
				for (int i = 0; i < 1000; i++) {
					initLatch.countDown();
				}

				log.error("", cause);
			}

			@Override
			public void onTimeout() {
				for (int i = 0; i < 1000; i++) {
					initLatch.countDown();
				}

				log.error("wait for reply of message [{}] timeout", SERVICE_CLIENT_INTERNAL_MESSAGE_QUERY_PRODUCT_BINDING);
			}

			@Override
			public void onComplete() {

			}

		});

		reply_timeout_tasks.put(messageId, submitReplyTimeoutTask(messageId));

		try {
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
			String curTime = LocalDateTime.now().format(formatter);

			ThingshubMessage thingshubMessage = new ThingshubMessage();
			thingshubMessage.setId(messageId);
			thingshubMessage.setClientId(clientId);
			thingshubMessage.setVersion(MESSAGE_MODEL_VERSION);
			thingshubMessage.setName(SERVICE_CLIENT_INTERNAL_MESSAGE_QUERY_PRODUCT_BINDING);
			thingshubMessage.setTime(curTime);
			thingshubMessage.setParams(new JSONObject());

			MqttMessage mqttMessage = new MqttMessage();
			mqttMessage.setQos(2);
			mqttMessage.setRetained(false);
			mqttMessage.setPayload(JSON.toJSONString(thingshubMessage).getBytes(StandardCharsets.UTF_8));

			IMqttToken token = mqttAsyncClient.publish(String.format(SERVICE_CLIENT_INTERNAL_TOPIC_QUERY_PRODUCT_BINDING, username, clientId), mqttMessage);
			token.waitForCompletion();
		} catch (MqttException e) {
			ReplyHandler<?> replyHandler = pending_reply_handlers.remove(messageId);
			if (replyHandler != null) {
				reply_timeout_tasks.remove(messageId).cancel(false);
				replyExecutor.submit(() -> replyHandler.onError(e));
			} else {
				log.error("", e);
			}
		}
	}

	private ScheduledFuture<?> submitReplyTimeoutTask(String messageId) {
		return timeoutScheduler.schedule(() -> {
			ReplyHandler<?> replyHandler = pending_reply_handlers.remove(messageId);
			if (replyHandler != null) {
				replyExecutor.submit(() -> replyHandler.onTimeout());
			}
			ScheduledFuture<?> timeoutFuture = reply_timeout_tasks.remove(messageId);
			if (timeoutFuture != null) {
				timeoutFuture.cancel(false);
			}
		}, defaultTimeout, TimeUnit.SECONDS);
	}

	private void queryMessageSpecs(String productCode) {
		String messageId = UUID.randomUUID().toString();

		pending_reply_handlers.put(messageId, new ReplyHandler<List<MessageSpec>>() {

			@Override
			public void onSuccess(List<MessageSpec> data) {
				if (!data.isEmpty()) {
					message_spec_table.row(data.get(0).getProductCode()).clear();

					for (MessageSpec item : data) {
						message_spec_table.put(item.getProductCode(), item.getName(), item);
					}
				}
			}

			@Override
			public void onError(Throwable cause) {
				log.error("", cause);
			}

			@Override
			public void onTimeout() {
				log.error("wait for reply of message [{}] timeout", SERVICE_CLIENT_INTERNAL_MESSAGE_QUERY_MESSAGE_DEFINITION);
			}

			@Override
			public void onComplete() {
				initLatch.countDown();
			}

		});

		reply_timeout_tasks.put(messageId, submitReplyTimeoutTask(messageId));

		try {
			JSONObject params = new JSONObject();
			params.put("productCode", productCode);

			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
			String curTime = LocalDateTime.now().format(formatter);

			ThingshubMessage thingshubMessage = new ThingshubMessage();
			thingshubMessage.setId(messageId);
			thingshubMessage.setClientId(clientId);
			thingshubMessage.setVersion(MESSAGE_MODEL_VERSION);
			thingshubMessage.setName(SERVICE_CLIENT_INTERNAL_MESSAGE_QUERY_MESSAGE_DEFINITION);
			thingshubMessage.setTime(curTime);
			thingshubMessage.setParams(params);

			MqttMessage mqttMessage = new MqttMessage();
			mqttMessage.setQos(2);
			mqttMessage.setRetained(false);
			mqttMessage.setPayload(JSON.toJSONString(thingshubMessage).getBytes(StandardCharsets.UTF_8));

			IMqttToken token = mqttAsyncClient.publish(String.format(SERVICE_CLIENT_INTERNAL_TOPIC_QUERY_MESSAGE_DEFINITION, username, clientId), mqttMessage);
			token.waitForCompletion();
		} catch (MqttException e) {
			ReplyHandler<?> replyHandler = pending_reply_handlers.remove(messageId);
			if (replyHandler != null) {
				reply_timeout_tasks.remove(messageId).cancel(false);
				replyExecutor.submit(() -> replyHandler.onError(e));
			} else {
				log.error("", e);
			}
		}
	}

	private void refreshProducts(String action, List<String> latestProductCodes) {
		Set<String> latestProductCodeSet = Sets.newHashSet(latestProductCodes);

		Sets.SetView<String> unboundProductCodes = Sets.difference(product_codes, latestProductCodeSet);
		unboundProductCodes.forEach(p -> {
			try {
				product_codes.remove(p);

				unsubscribe(String.format(THING_TOPIC_PROPERTY_POST, p, "+", "+"));
				unsubscribe(String.format(THING_TOPIC_SERVICE_FUNCTION_REPLY, p, "+", "+"));
				unsubscribe(String.format(THING_TOPIC_SERVICE_REQUEST_POST, p, "+", "+"));
				unsubscribe(String.format(THING_TOPIC_EVENT_POST, p, "+", "+"));

				message_spec_table.row(p).clear();
			} catch (ThingshubException e) {
				log.error("", e);
			}
		});

		Sets.SetView<String> addedProductCodes = Sets.difference(latestProductCodeSet, product_codes);
		addedProductCodes.forEach(p -> {
			try {
				product_codes.add(p);

				subscribe(String.format(THING_TOPIC_PROPERTY_POST, p, "+", "+"), 2);
				subscribe(String.format(THING_TOPIC_SERVICE_FUNCTION_REPLY, p, "+", "+"), 2);
				subscribe(String.format(THING_TOPIC_SERVICE_REQUEST_POST, p, "+", "+"), 2);
				subscribe(String.format(THING_TOPIC_EVENT_POST, p, "+", "+"), 2);
			} catch (ThingshubException e) {
				log.error("", e);
			}
		});
	}

	private void refreshMessageSpec(String action, String messageName, MessageSpec messageSpec) {
		if ("CREATED".equals(action) || "UPDATED".equals(action)) {
			message_spec_table.put(messageSpec.getProductCode(), messageName, messageSpec);
		} else if ("REMOVED".equals(action)) {
			message_spec_table.remove(messageSpec.getProductCode(), messageName);
		}
	}

	private void subscribe(String topic, int qos) {
		try {
			IMqttToken token = mqttAsyncClient.subscribe(topic, qos);
			token.waitForCompletion();

			log.info("{}({}) subscribe topic: {}, QoS: {}", username, clientId, topic, qos);
		} catch (MqttException e) {
			log.error("{}({}) subscribe error, topic: {}, QoS: {}", username, clientId, topic, qos);
			log.error("", e);
			throw new ThingshubException(e.getMessage());
		}
	}

	private void unsubscribe(String topic) {
		try {
			IMqttToken token = mqttAsyncClient.unsubscribe(topic);
			token.waitForCompletion();

			log.info("{}({}) unsubscribe: {}", username, clientId, topic);
		} catch (MqttException e) {
			log.error("{}({}) unsubscribe error: {}", username, clientId, topic);
			log.error("", e);
			throw new ThingshubException(e.getMessage());
		}
	}

	/**
	 * 发送消息。将消息发送给产品的所有设备
	 * 
	 * @param productCode 产品编码
	 * @param messageName 消息名称
	 * @param params      消息参数
	 */
	public void publish(String productCode, String messageName, Object params) {
		this.publish(productCode, "+", messageName, params);
	}

//	/**
//	 * 发送消息。将消息发送给产品的所有设备，并通过ReplyHandler处理设备的回复消息
//	 * 
//	 * @param productCode  产品编码
//	 * @param messageName  消息名称
//	 * @param params       消息参数
//	 * @param replyHandler 回复处理器
//	 */
//	public <R> void publishToAll(String productCode, String messageName, Object params, ReplyHandler<R> replyHandler) {
//		this.publish(productCode, "+", messageName, params, replyHandler);
//	}

	/**
	 * 发送消息。将消息发送给指定设备
	 * 
	 * @param productCode 产品编码
	 * @param sn          设备序列号
	 * @param messageName 消息名称
	 * @param params      消息参数
	 */
	public void publish(String productCode, String sn, String messageName, Object params) {
		try {
			if (!initLatch.await(15, TimeUnit.SECONDS)) {
				throw new ThingshubException("await thingshub client's initialization timeout");
			}
		} catch (InterruptedException e) {
			log.error("", e);
			throw new ThingshubException(e.getMessage());
		}

		if (product_codes.isEmpty() || !product_codes.contains(productCode)) {
			throw new ThingshubException(String.format("未绑定产品[%s]", productCode));
		}

		MessageSpec messageSpec = message_spec_table.get(productCode, messageName);
		if (messageSpec == null) {
			throw new ThingshubException(String.format("产品[%s]消息[%s]未定义或没有权限", productCode, messageName));
		}

		List<MessageParameter> parametersInSpec = messageSpec.getType().equals(SERVICE_FUNCTION_CALL.name()) ? messageSpec.getParameters() : Collections.emptyList();
		if (params != null) {
			if (params instanceof List paramItems) {
				for (Object paramItem : paramItems) {
					validateMessageParameter(paramItem, parametersInSpec);
				}
			} else {
				validateMessageParameter(params, parametersInSpec);
			}
		}

		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
		String curTime = LocalDateTime.now().format(formatter);
		String messageId = UUID.randomUUID().toString();

		ThingshubMessage thingshubMessage = new ThingshubMessage();
		thingshubMessage.setId(messageId);
		thingshubMessage.setClientId(clientId);
		thingshubMessage.setVersion(MESSAGE_MODEL_VERSION);
		thingshubMessage.setName(messageName);
		thingshubMessage.setTime(curTime);
		thingshubMessage.setParams(params);
		String msgTopic = String.format(THING_TOPIC_SERVICE_FUNCTION_CALL, messageSpec.getProductCode(), sn, messageSpec.getName());

		MqttMessage mqttMessage = new MqttMessage();
		mqttMessage.setQos(2);
		mqttMessage.setRetained(false);
		mqttMessage.setPayload(JSON.toJSONString(thingshubMessage).getBytes(StandardCharsets.UTF_8));

		try {
			IMqttToken token = mqttAsyncClient.publish(msgTopic, mqttMessage);
			token.waitForCompletion();
		} catch (MqttException e) {
			log.error("publish failed, message name: {}, message id: {}, error: ", messageId, messageName, e);
			throw new ThingshubException(e.getMessage());
		}
	}

//	/**
//	 * 发送消息。将消息发送给指定设备，并通过ReplyHandler处理设备的回复消息
//	 * 
//	 * @param productCode  产品编码
//	 * @param sn           设备序列号
//	 * @param messageName  消息名称
//	 * @param params       消息参数
//	 * @param replyHandler 回复处理器
//	 */
//	public <R> void publish(String productCode, String sn, String messageName, Object params, ReplyHandler<R> replyHandler) {
//		try {
//			if (!initLatch.await(15, TimeUnit.SECONDS)) {
//				throw new ThingshubException("await thingshub client's initialization timeout");
//			}
//		} catch (InterruptedException e) {
//			log.error("", e);
//			throw new ThingshubException(e.getMessage());
//		}
//
//		if (product_codes.isEmpty() || !product_codes.contains(productCode)) {
//			throw new ThingshubException(String.format("未绑定产品[%s]", productCode));
//		}
//
//		MessageSpec messageSpec = message_spec_table.get(productCode, messageName);
//		if (messageSpec == null) {
//			throw new ThingshubException(String.format("产品[%s]消息[%s]未定义或没有权限", productCode, messageName));
//		}
//
//		List<MessageParameter> parametersInSpec = messageSpec.getType().equals(SERVICE_FUNCTION_CALL.name()) ? messageSpec.getParameters() : Collections.emptyList();
//		if (params != null) {
//			if (params instanceof List paramItems) {
//				for (Object paramItem : paramItems) {
//					validateMessageParameter(paramItem, parametersInSpec);
//				}
//			} else {
//				validateMessageParameter(params, parametersInSpec);
//			}
//		}
//
//		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
//		String curTime = LocalDateTime.now().format(formatter);
//
//		String messageId = UUID.randomUUID().toString();
//		if (replyHandler != null) {
//			pending_reply_handlers.put(messageId, replyHandler);
//			reply_timeout_tasks.put(messageId, submitReplyTimeoutTask(messageId));
//		}
//
//		ThingshubMessage thingshubMessage = new ThingshubMessage();
//		thingshubMessage.setId(messageId);
//		thingshubMessage.setClientId(clientId);
//		thingshubMessage.setVersion(MESSAGE_MODEL_VERSION);
//		thingshubMessage.setName(messageName);
//		thingshubMessage.setTime(curTime);
//		thingshubMessage.setParams(params);
//		String msgTopic = String.format(THING_TOPIC_SERVICE_FUNCTION_CALL, messageSpec.getProductCode(), sn, messageSpec.getName());
//
//		MqttMessage mqttMessage = new MqttMessage();
//		mqttMessage.setQos(2);
//		mqttMessage.setRetained(false);
//		mqttMessage.setPayload(JSON.toJSONString(thingshubMessage).getBytes(StandardCharsets.UTF_8));
//
//		try {
//			IMqttToken token = mqttAsyncClient.publish(msgTopic, mqttMessage);
//			token.waitForCompletion();
//		} catch (MqttException e) {
//			ReplyHandler<?> theReplyHandler = pending_reply_handlers.remove(messageId);
//			if (theReplyHandler != null) {
//				reply_timeout_tasks.remove(messageId).cancel(false);
//				replyExecutor.submit(() -> theReplyHandler.onError(e));
//			} else {
//				log.error("publish failed, message name: {}, message id: {}, error: ", messageId, messageName, e);
//				throw new ThingshubException(e.getMessage());
//			}
//		}
//	}

	private void validateMessageParameter(Object paramObj, List<MessageParameter> parametersInSpec) {
		JSONObject paramObjInJSON = JSONObject.from(paramObj);
		Set<String> paramIdsInPayload = paramObjInJSON.keySet();
		Set<String> paramIdsInSpec = Sets.newHashSet();

		for (MessageParameter mp : parametersInSpec) {
			paramIdsInSpec.add(mp.getIdentifier());

			DataType parameterDataTypeSpecs = DataType.from(mp.getDataTypeSpecs());
			if (parameterDataTypeSpecs != null) {
				Object parameterValue = paramObjInJSON.get(mp.getIdentifier());
				if (parameterValue != null) {
					ValidateResult vResult = parameterDataTypeSpecs.validate(parameterValue);
					if (!vResult.isSuccess()) {
						throw new ThingshubException(String.format("参数[%s]错误: %s", mp.getIdentifier(), vResult.getError()));
					}
				} else {
					throw new ThingshubException(String.format("参数[%s]必需", mp.getIdentifier()));
				}
			}
		}

		Sets.SetView<String> illegalParams = Sets.difference(paramIdsInPayload, paramIdsInSpec);
		if (illegalParams != null && !illegalParams.isEmpty()) {
			throw new ThingshubException(String.format("非法参数%s", illegalParams.toString()));
		}
	}

	/**
	 * 回复消息。将回复消息发送给指定设备
	 * 
	 * @param productCode 产品编码
	 * @param sn          设备序列号
	 * @param msgName     消息名称
	 * @param msgId       消息ID
	 * @param data        回复内容
	 */
	public void reply(String productCode, String sn, String msgName, String msgId, Object data) {
		try {
			if (!initLatch.await(15, TimeUnit.SECONDS)) {
				throw new ThingshubException("await thingshub client's initialization timeout");
			}
		} catch (InterruptedException e) {
			log.error("", e);
			throw new ThingshubException(e.getMessage());
		}

		if (product_codes.isEmpty() || !product_codes.contains(productCode)) {
			throw new ThingshubException(String.format("未绑定产品[%s]", productCode));
		}

		MessageSpec messageSpec = message_spec_table.get(productCode, msgName);
		if (messageSpec == null) {
			throw new ThingshubException(String.format("产品[%s]消息名称[%s]未定义", productCode, msgName));
		}

		List<MessageParameter> parametersInSpec = messageSpec.getType().equals(SERVICE_REQUEST_REPLY.name()) ? messageSpec.getParameters() : Collections.emptyList();
		if (data != null) {
			if (data instanceof List dataItems) {
				for (Object dataItem : dataItems) {
					validateMessageParameter(dataItem, parametersInSpec);
				}
			} else {
				validateMessageParameter(data, parametersInSpec);
			}
		}

		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
		String curTime = LocalDateTime.now().format(formatter);

		ThingshubMessage thingshubMessage = new ThingshubMessage();
		thingshubMessage.setId(msgId);
		thingshubMessage.setClientId(clientId);
		thingshubMessage.setVersion(MESSAGE_MODEL_VERSION);
		thingshubMessage.setName(msgName);
		thingshubMessage.setTime(curTime);
		thingshubMessage.setCode(MessageResult.SUCCESS.code());
		thingshubMessage.setMessage(MessageResult.SUCCESS.desc());
		thingshubMessage.setData(data);

		MqttMessage mqttMessage = new MqttMessage();
		mqttMessage.setQos(2);
		mqttMessage.setRetained(false);
		mqttMessage.setPayload(JSON.toJSONString(thingshubMessage).getBytes(StandardCharsets.UTF_8));

		try {
			String msgTopic = switch (MessageType.of(messageSpec.getType())) {
			case PROPERTY_REPLY -> String.format(THING_TOPIC_PROPERTY_REPLY, productCode, sn, msgName);
			case SERVICE_REQUEST_REPLY -> String.format(THING_TOPIC_SERVICE_REQUEST_REPLY, productCode, sn, msgName);
			case EVENT_REPLY -> String.format(THING_TOPIC_EVENT_REPLY, productCode, sn, msgName);
			default -> throw new ThingshubException("invalid message type " + messageSpec.getType());
			};

			IMqttToken token = mqttAsyncClient.publish(msgTopic, mqttMessage);
			token.waitForCompletion();
		} catch (MqttException e) {
			log.error("reply failed, message name: {}, message id: {}, error: ", msgId, msgName, e);
			throw new ThingshubException(e.getMessage());
		}
	}

	/**
	 * 回复错误消息。将消息发送给指定设备
	 * 
	 * @param productCode 产品编码
	 * @param sn          设备序列号
	 * @param msgName     消息名称
	 * @param msgId       消息ID
	 * @param errCode     错误代码
	 * @param errMsg      错误信息
	 */
	public void replyWithError(String productCode, String sn, String msgName, String msgId, Integer errCode, String errMsg) {
		try {
			if (!initLatch.await(15, TimeUnit.SECONDS)) {
				throw new ThingshubException("await thingshub client's initialization timeout");
			}
		} catch (InterruptedException e) {
			log.error("", e);
			throw new ThingshubException(e.getMessage());
		}

		if (product_codes.isEmpty() || !product_codes.contains(productCode)) {
			throw new ThingshubException(String.format("未绑定产品[%s]", productCode));
		}

		MessageSpec messageSpec = message_spec_table.get(productCode, msgName);
		if (messageSpec == null) {
			throw new ThingshubException(String.format("产品[%s]消息名称[%s]未定义", productCode, msgName));
		}

		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
		String curTime = LocalDateTime.now().format(formatter);

		ThingshubMessage thingshubMessage = new ThingshubMessage();
		thingshubMessage.setId(msgId);
		thingshubMessage.setClientId(clientId);
		thingshubMessage.setVersion(MESSAGE_MODEL_VERSION);
		thingshubMessage.setName(msgName);
		thingshubMessage.setTime(curTime);
		thingshubMessage.setCode(errCode);
		thingshubMessage.setMessage(errMsg);

		MqttMessage mqttMessage = new MqttMessage();
		mqttMessage.setQos(2);
		mqttMessage.setRetained(false);
		mqttMessage.setPayload(JSON.toJSONString(thingshubMessage).getBytes(StandardCharsets.UTF_8));

		try {
			String msgTopic = switch (MessageType.of(messageSpec.getType())) {
			case PROPERTY_REPLY -> String.format(THING_TOPIC_PROPERTY_REPLY, productCode, sn, msgName);
			case SERVICE_REQUEST_REPLY -> String.format(THING_TOPIC_SERVICE_REQUEST_REPLY, productCode, sn, msgName);
			case EVENT_REPLY -> String.format(THING_TOPIC_EVENT_REPLY, productCode, sn, msgName);
			default -> throw new ThingshubException("invalid message type " + messageSpec.getType());
			};

			IMqttToken token = mqttAsyncClient.publish(msgTopic, mqttMessage);
			token.waitForCompletion();
		} catch (MqttException e) {
			log.error("reply failed, message name: {}, message id: {}, error: ", msgId, msgName, e);
			throw new ThingshubException(e.getMessage());
		}
	}

	private void disconnect() {
		if (mqttAsyncClient != null && mqttAsyncClient.isConnected()) {
			try {
				log.info("{}({}) disconnecting", username, clientId);
				IMqttToken token = mqttAsyncClient.disconnect();
				token.waitForCompletion();
				log.info("{}({}) disconnected", username, clientId);
			} catch (MqttException e) {
				log.error("{}({}) disconnecting failed: ", username, clientId, e);
			}
		}
		mqttAsyncClient = null;
	}

	/**
	 * 获取在产品上定义的服务
	 * 
	 * @param productCode
	 * @return
	 */
	public List<ThingServiceItem> getServiceFunctions(String productCode) {
		Map<String, MessageSpec> messageSpecs = message_spec_table.row(productCode);
		if (messageSpecs == null) {
			return Collections.emptyList();
		}

		return messageSpecs.values().stream().filter(m -> m.getType().equals(SERVICE_FUNCTION_CALL.name())).map(m -> {
			ThingServiceItem command = new ThingServiceItem();
			command.setName(m.getName());
			command.setTitle(m.getTitle());
			command.setParameters(m.getParameters());

			return command;
		}).toList();
	};

	/**
	 * 查询设备
	 * 
	 * @param queryCriterions
	 * @return
	 */
	public Page<DeviceInfo> queryDevice(DeviceQueryCriterions queryCriterions) {
		try {
			if (!initLatch.await(15, TimeUnit.SECONDS)) {
				throw new ThingshubException("await thingshub client's initialization timeout");
			}
		} catch (InterruptedException e) {
			log.error("", e);
			throw new ThingshubException(e.getMessage());
		}

		String messageId = UUID.randomUUID().toString();

		CompletableFuture<Page<DeviceInfo>> future = new CompletableFuture<>();
		pending_reply_handlers.put(messageId, new ReplyHandler<Page<DeviceInfo>>() {

			@Override
			public void onSuccess(Page<DeviceInfo> data) {
				future.complete(data);
			}

			@Override
			public void onError(Throwable cause) {
				log.error("query device error: ", cause);
				future.completeExceptionally(cause);
			}

			@Override
			public void onTimeout() {
				log.error("wait for response of message [{}] timeout", SERVICE_CLIENT_INTERNAL_MESSAGE_QUERY_DEVICE);
				future.completeExceptionally(new ThingshubException("wait for response timeout after " + defaultTimeout + " seconds"));
			}

			@Override
			public void onComplete() {

			}

		});
		reply_timeout_tasks.put(messageId, submitReplyTimeoutTask(messageId));

		try {
			JSONObject params = new JSONObject();
			params.put("queryCriterions", queryCriterions);

			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
			String curTime = LocalDateTime.now().format(formatter);

			ThingshubMessage thingshubMessage = new ThingshubMessage();
			thingshubMessage.setId(messageId);
			thingshubMessage.setClientId(clientId);
			thingshubMessage.setVersion(MESSAGE_MODEL_VERSION);
			thingshubMessage.setName(SERVICE_CLIENT_INTERNAL_MESSAGE_QUERY_DEVICE);
			thingshubMessage.setTime(curTime);
			thingshubMessage.setParams(params);

			MqttMessage mqttMessage = new MqttMessage();
			mqttMessage.setQos(2);
			mqttMessage.setRetained(false);
			mqttMessage.setPayload(JSON.toJSONString(thingshubMessage).getBytes(StandardCharsets.UTF_8));

			IMqttToken token = mqttAsyncClient.publish(String.format(SERVICE_CLIENT_INTERNAL_TOPIC_QUERY_DEVICE, username, clientId), mqttMessage);
			token.waitForCompletion();

			return future.get();
		} catch (MqttException | InterruptedException | ExecutionException e) {
			ReplyHandler<?> replyHandler = pending_reply_handlers.remove(messageId);
			if (replyHandler != null) {
				reply_timeout_tasks.remove(messageId).cancel(false);
				replyExecutor.submit(() -> replyHandler.onError(e));
			} else {
				log.error("", e);
			}

			throw new ThingshubException(e.getMessage());
		}
	}

	/**
	 * 获取设备信息
	 * 
	 * @param productCode 产品编码
	 * @param sn          设备序列号
	 * @return
	 */
	public DeviceInfo getDeviceInfo(String productCode, String sn) {
		try {
			if (!initLatch.await(15, TimeUnit.SECONDS)) {
				throw new ThingshubException("await thingshub client's initialization timeout");
			}
		} catch (InterruptedException e) {
			log.error("", e);
			throw new ThingshubException(e.getMessage());
		}

		String messageId = UUID.randomUUID().toString();

		CompletableFuture<DeviceInfo> future = new CompletableFuture<>();
		pending_reply_handlers.put(messageId, new ReplyHandler<DeviceInfo>() {

			@Override
			public void onSuccess(DeviceInfo data) {
				future.complete(data);
			}

			@Override
			public void onError(Throwable cause) {
				log.error("", cause);
				future.completeExceptionally(cause);
			}

			@Override
			public void onTimeout() {
				log.error("wait for response of message [{}] timeout", SERVICE_CLIENT_INTERNAL_MESSAGE_DEVICE_INFO);
				future.completeExceptionally(new ThingshubException("wait for response timeout after " + defaultTimeout + " seconds"));
			}

			@Override
			public void onComplete() {

			}

		});
		reply_timeout_tasks.put(messageId, submitReplyTimeoutTask(messageId));

		try {
			JSONObject params = new JSONObject();
			params.put("productCode", productCode);
			params.put("sn", sn);

			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
			String curTime = LocalDateTime.now().format(formatter);

			ThingshubMessage thingshubMessage = new ThingshubMessage();
			thingshubMessage.setId(messageId);
			thingshubMessage.setClientId(clientId);
			thingshubMessage.setVersion(MESSAGE_MODEL_VERSION);
			thingshubMessage.setName(SERVICE_CLIENT_INTERNAL_MESSAGE_DEVICE_INFO);
			thingshubMessage.setTime(curTime);
			thingshubMessage.setParams(params);

			MqttMessage mqttMessage = new MqttMessage();
			mqttMessage.setQos(2);
			mqttMessage.setRetained(false);
			mqttMessage.setPayload(JSON.toJSONString(thingshubMessage).getBytes(StandardCharsets.UTF_8));

			IMqttToken token = mqttAsyncClient.publish(String.format(SERVICE_CLIENT_INTERNAL_TOPIC_DEVICE_INFO, username, clientId), mqttMessage);
			token.waitForCompletion();

			return future.get();
		} catch (MqttException | InterruptedException | ExecutionException e) {
			ReplyHandler<?> replyHandler = pending_reply_handlers.remove(messageId);
			if (replyHandler != null) {
				reply_timeout_tasks.remove(messageId).cancel(false);
				replyExecutor.submit(() -> replyHandler.onError(e));
			} else {
				log.error("", e);
			}

			throw new ThingshubException(e.getMessage());
		}
	}

	@PreDestroy
	public void shutdown() {
		if (mqttAsyncClient != null && mqttAsyncClient.isConnected()) {
			unsubscribe(String.format(SERVICE_CLIENT_INTERNAL_TOPIC_CHANGE_PRODUCT_BINDING, username, "+"));
			unsubscribe(String.format(SERVICE_CLIENT_INTERNAL_TOPIC_CHANGE_MESSAGE_AUTHORIZATION, username, "+"));
			unsubscribe(String.format(SERVICE_CLIENT_INTERNAL_TOPIC_QUERY_PRODUCT_BINDING_REPLY, username, clientId));
			unsubscribe(String.format(SERVICE_CLIENT_INTERNAL_TOPIC_QUERY_MESSAGE_DEFINITION_REPLY, username, clientId));
			unsubscribe(String.format(SERVICE_CLIENT_INTERNAL_TOPIC_QUERY_DEVICE_REPLY, username, clientId));
			unsubscribe(String.format(SERVICE_CLIENT_INTERNAL_TOPIC_DEVICE_INFO_REPLY, username, clientId));
		}

		product_codes.forEach(p -> {
			if (mqttAsyncClient != null && mqttAsyncClient.isConnected()) {
				try {
					unsubscribe(String.format(THING_TOPIC_PROPERTY_POST, p, "+", "+"));
					unsubscribe(String.format(THING_TOPIC_SERVICE_FUNCTION_REPLY, p, "+", "+"));
					unsubscribe(String.format(THING_TOPIC_SERVICE_REQUEST_POST, p, "+", "+"));
					unsubscribe(String.format(THING_TOPIC_EVENT_POST, p, "+", "+"));
				} catch (ThingshubException e) {
					log.error("", e);
				}
			}
		});

		this.disconnect();
	}

}
