package io.thingshub.client;

import static io.thingshub.commons.model.MessageType.REPLY;
import static io.thingshub.commons.model.MessageType.REQUEST;
import static io.thingshub.commons.model.ThingModelType.SERVICE;
import static io.thingshub.commons.model.ThingshubConstants.INTERNAL_MESSAGE_DEVICE_INFO;
import static io.thingshub.commons.model.ThingshubConstants.INTERNAL_MESSAGE_DISABLE_DEVICE;
import static io.thingshub.commons.model.ThingshubConstants.INTERNAL_MESSAGE_ENABLE_DEVICE;
import static io.thingshub.commons.model.ThingshubConstants.INTERNAL_MESSAGE_QUERY_BOUND_PRODUCT;
import static io.thingshub.commons.model.ThingshubConstants.INTERNAL_MESSAGE_QUERY_DEVICE;
import static io.thingshub.commons.model.ThingshubConstants.INTERNAL_MESSAGE_QUERY_MESSAGE_MODEL;
import static io.thingshub.commons.model.ThingshubConstants.INTERNAL_TOPIC_BOUND_PRODUCT_CHANGED;
import static io.thingshub.commons.model.ThingshubConstants.INTERNAL_TOPIC_BOUND_PRODUCT_QUERY;
import static io.thingshub.commons.model.ThingshubConstants.INTERNAL_TOPIC_BOUND_PRODUCT_QUERY_REPLY;
import static io.thingshub.commons.model.ThingshubConstants.INTERNAL_TOPIC_DEVICE_DISABLE;
import static io.thingshub.commons.model.ThingshubConstants.INTERNAL_TOPIC_DEVICE_DISABLE_REPLY;
import static io.thingshub.commons.model.ThingshubConstants.INTERNAL_TOPIC_DEVICE_ENABLE;
import static io.thingshub.commons.model.ThingshubConstants.INTERNAL_TOPIC_DEVICE_ENABLE_REPLY;
import static io.thingshub.commons.model.ThingshubConstants.INTERNAL_TOPIC_DEVICE_INFO;
import static io.thingshub.commons.model.ThingshubConstants.INTERNAL_TOPIC_DEVICE_INFO_REPLY;
import static io.thingshub.commons.model.ThingshubConstants.INTERNAL_TOPIC_DEVICE_QUERY;
import static io.thingshub.commons.model.ThingshubConstants.INTERNAL_TOPIC_DEVICE_QUERY_REPLY;
import static io.thingshub.commons.model.ThingshubConstants.INTERNAL_TOPIC_MESSAGE_MODEL_CHANGED;
import static io.thingshub.commons.model.ThingshubConstants.INTERNAL_TOPIC_MESSAGE_MODEL_QUERY;
import static io.thingshub.commons.model.ThingshubConstants.INTERNAL_TOPIC_MESSAGE_MODEL_QUERY_REPLY;
import static io.thingshub.commons.model.ThingshubConstants.THING_EVENT_POST_REPLY_TOPIC_FORMAT;
import static io.thingshub.commons.model.ThingshubConstants.THING_EVENT_POST_TOPIC_FORMAT;
import static io.thingshub.commons.model.ThingshubConstants.THING_PROPERTY_POST_REPLY_TOPIC_FORMAT;
import static io.thingshub.commons.model.ThingshubConstants.THING_PROPERTY_POST_TOPIC_FORMAT;
import static io.thingshub.commons.model.ThingshubConstants.THING_SERVICE_CALL_REPLY_TOPIC_FORMAT;
import static io.thingshub.commons.model.ThingshubConstants.THING_SERVICE_CALL_TOPIC_FORMAT;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.eclipse.paho.mqttv5.client.MqttCallback;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.TypeReference;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

import io.thingshub.client.model.DeviceInfo;
import io.thingshub.client.model.DeviceQueryCriterions;
import io.thingshub.client.model.ThingServiceItem;
import io.thingshub.commons.meta.ValidateResult;
import io.thingshub.commons.meta.types.DataType;
import io.thingshub.commons.model.MessageModel;
import io.thingshub.commons.model.MessageParameter;
import io.thingshub.commons.model.MessageResult;
import io.thingshub.commons.model.Page;
import io.thingshub.commons.model.ThingModelType;
import io.thingshub.commons.model.ThingshubMessage;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ThingshubClient {

	private static final String MESSAGE_MODEL_VERSION = "1.0.0";

	private static final String PREFIX_SHARE_TOPIC = "$share/";

	private final Set<String> product_codes = Sets.newConcurrentHashSet();
	private final Table<String, String, MessageModel> message_model_table = HashBasedTable.create();

	private final Map<String, ReplyHandler<?>> pending_reply_handlers = Maps.newConcurrentMap();
	private final Map<String, ScheduledFuture<?>> reply_timeout_futures = Maps.newConcurrentMap();

	private final ScheduledExecutorService timeoutScheduler = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors() + 1);
	private final ExecutorService replyExecutor = Executors.newCachedThreadPool(r -> new Thread(r, "thingshub-reply-handler-" + UUID.randomUUID().toString().substring(0, 8)));

	private final ClientConfig clientConfig;

	private int defaultTimeout = 15;// seconds

	private MqttAsyncClient mqttAsyncClient;

	private MessageListener messageListener;

	/**
	 * 
	 * @param host            服务器地址
	 * @param port            服务器端口
	 * @param username        用户名
	 * @param password        密码
	 * @param messageListener 设备消息监听器
	 */
	public ThingshubClient(String host, Integer port, String username, String password, MessageListener messageListener) {
		if (host == null) {
			throw new ThingshubException("host不能为空");
		}
		if (port == null) {
			throw new ThingshubException("port不能为空");
		}
		if (username == null) {
			throw new ThingshubException("username不能为空");
		}
		if (password == null) {
			throw new ThingshubException("password不能为空");
		}

		String clientId = "thingshub-client-" + ThreadLocalRandom.current().nextInt(10000, 100000);
		this.clientConfig = ClientConfig.builder().host(host).port(port).username(username).password(password).clientId(clientId).build();
		this.messageListener = messageListener;

		start();
	}

	/**
	 * 
	 * @param host            服务器地址
	 * @param port            服务器端口
	 * @param username        用户名
	 * @param password        密码
	 * @param clientId        Client ID。默认由系统分配
	 * @param cleanStart      是否启用Clean Session。默认为true
	 * @param messageListener 设备消息监听器
	 */
	public ThingshubClient(String host, Integer port, String username, String password, String clientId, Boolean cleanStart, MessageListener messageListener) {
		if (host == null) {
			throw new ThingshubException("host不能为空");
		}
		if (port == null) {
			throw new ThingshubException("port不能为空");
		}
		if (username == null) {
			throw new ThingshubException("username不能为空");
		}
		if (password == null) {
			throw new ThingshubException("password不能为空");
		}

		if (clientId == null) {
			clientId = "thingshub-client-" + ThreadLocalRandom.current().nextInt(10000, 100000);
		}
		if (cleanStart == null) {
			cleanStart = true;
		}

		this.clientConfig = ClientConfig.builder().host(host).port(port).username(username).password(password).clientId(clientId).cleanStart(cleanStart).build();
		this.messageListener = messageListener;

		start();
	}

	/**
	 * 
	 * @param clientConfig 配置信息
	 */
	public ThingshubClient(ClientConfig clientConfig, MessageListener messageListener) {
		if (clientConfig == null) {
			throw new ThingshubException("配置信息不能为空");
		}

		if (clientConfig.getHost() == null) {
			throw new ThingshubException("host不能为空");
		}
		if (clientConfig.getPort() == null) {
			throw new ThingshubException("port不能为空");
		}
		if (clientConfig.getUsername() == null) {
			throw new ThingshubException("username不能为空");
		}
		if (clientConfig.getPassword() == null) {
			throw new ThingshubException("password不能为空");
		}

		if (clientConfig.getClientId() == null) {
			String clientId = "thingshub-client-" + ThreadLocalRandom.current().nextInt(10000, 100000);
			this.clientConfig = ClientConfig.builder().host(clientConfig.getHost()).port(clientConfig.getPort()).username(clientConfig.getUsername())
					.password(clientConfig.getPassword()).clientId(clientId).build();
		} else {
			this.clientConfig = clientConfig;
		}

		this.messageListener = messageListener;

		start();
	}

	private void start() {
		MqttConnectionOptions opts = new MqttConnectionOptions();
		opts.setUserName(clientConfig.getUsername());
		opts.setPassword(Optional.ofNullable(clientConfig.getPassword()).orElseGet(() -> "").getBytes(StandardCharsets.UTF_8));
		opts.setCleanStart(clientConfig.getCleanStart());
		opts.setConnectionTimeout(clientConfig.getConnectTimeout().intValue());
		opts.setMaxReconnectDelay(clientConfig.getMaxReconnectDelay().intValue() * 1000);
		opts.setKeepAliveInterval(clientConfig.getKeepAlive().intValue());
		opts.setAutomaticReconnect(true);

		try {
			String serverUri = String.format("tcp://%s:%d", clientConfig.getHost(), clientConfig.getPort());
			mqttAsyncClient = new MqttAsyncClient(serverUri, clientConfig.getClientId(), new MemoryPersistence());
			mqttAsyncClient.setCallback(new MqttCallback() {

				@Override
				public void disconnected(MqttDisconnectResponse disconnectResponse) {
					log.warn("{} disconnected from thingshub. code: {}, reason: {}", clientConfig.getClientId(), disconnectResponse.getReturnCode(),
							disconnectResponse.getReasonString());

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
					if (thingshubMessage.getCode() != null) {// reply message
						ReplyHandler<?> replyHandler = pending_reply_handlers.remove(thingshubMessage.getId());
						if (replyHandler == null) {
							log.warn("No reply handler for message[{}, {}]", thingshubMessage.getName(), thingshubMessage.getId());

							return;
						}

						reply_timeout_futures.remove(thingshubMessage.getId()).cancel(false);

						handleReply(thingshubMessage, replyHandler);
					} else if (thingshubMessage.getName().equals(INTERNAL_TOPIC_BOUND_PRODUCT_CHANGED)) {
						JSONObject params = (JSONObject) thingshubMessage.getParams();
						String action = params.getString("action");
						List<String> productCodes = params.getList("productCodes", String.class);

						refreshProducts(action, productCodes);
					} else if (thingshubMessage.getName().equals(INTERNAL_TOPIC_MESSAGE_MODEL_CHANGED)) {
						JSONObject params = (JSONObject) thingshubMessage.getParams();
						String action = params.getString("action");
						MessageModel messageModel = params.getObject("messageModel", MessageModel.class);

						refreshMessageModel(action, messageModel);
					} else {
						Message message = Message.builder().id(thingshubMessage.getId()).name(thingshubMessage.getName()).sn(thingshubMessage.getClientId())
								.params(thingshubMessage.getParams()).build();
						messageListener.onMessage(message);
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
					log.info("{} has connected to thingshub mqtt server", clientConfig.getClientId());

					subscribe(String.format(INTERNAL_TOPIC_BOUND_PRODUCT_CHANGED, clientConfig.getUsername()), 2);
					subscribe(String.format(INTERNAL_TOPIC_BOUND_PRODUCT_QUERY_REPLY, clientConfig.getUsername(), clientConfig.getClientId()), 2);
					subscribe(String.format(INTERNAL_TOPIC_MESSAGE_MODEL_QUERY_REPLY, clientConfig.getUsername(), clientConfig.getClientId()), 2);
					subscribe(String.format(INTERNAL_TOPIC_DEVICE_QUERY_REPLY, clientConfig.getUsername(), clientConfig.getClientId()), 2);
					subscribe(String.format(INTERNAL_TOPIC_DEVICE_INFO_REPLY, clientConfig.getUsername(), clientConfig.getClientId()), 2);
					subscribe(String.format(INTERNAL_TOPIC_DEVICE_DISABLE_REPLY, clientConfig.getUsername(), clientConfig.getClientId()), 2);
					subscribe(String.format(INTERNAL_TOPIC_DEVICE_ENABLE_REPLY, clientConfig.getUsername(), clientConfig.getClientId()), 2);

					queryBoundProducts();
				}

				@Override
				public void authPacketArrived(int reasonCode, MqttProperties properties) {

				}

			});
		} catch (MqttException e) {
			log.error("", e);

			throw new ThingshubException("创建Thingshub客户端出错：" + e.getMessage());
		}

		if (mqttAsyncClient != null) {
			while (!mqttAsyncClient.isConnected()) {
				try {
					IMqttToken token = mqttAsyncClient.connect(opts);
					token.waitForCompletion();
				} catch (Exception e) {
					log.error("", e);

					throw new ThingshubException("连接Thingshub MQTT服务器出错：" + e.getMessage());
				}
			}
		}
	}

	private <T> void handleReply(ThingshubMessage thingshubMessage, ReplyHandler<T> replyHandler) {
		replyExecutor.submit(() -> {
			try {
				if (thingshubMessage.getCode().intValue() == MessageResult.SUCCESS.code()) {
					if (thingshubMessage.getData() != null) {
						String dataStr = JSON.toJSONString(thingshubMessage.getData());
						T dataObj = JSON.parseObject(dataStr, replyHandler.getType());
						replyHandler.onSuccess(dataObj);
					} else {
						replyHandler.onSuccess(null);
					}
				} else {
					replyHandler.onFailure(new ThingshubException(thingshubMessage.getMessage()));
				}
			} catch (Exception e) {
				log.error("", e);

				replyHandler.onFailure(e);
			} finally {
				replyHandler.onComplete();
			}
		});
	}

	private void queryBoundProducts() {
		String messageId = UUID.randomUUID().toString();

		pending_reply_handlers.put(messageId, new ReplyHandler<List<String>>() {

			@Override
			public Type getType() {
				return new TypeReference<List<String>>() {
				}.getType();
			}

			@Override
			public void onSuccess(List<String> data) {
				for (String productCode : data) {
					product_codes.add(productCode);

					subscribe(PREFIX_SHARE_TOPIC + clientConfig.getUsername() + String.format(THING_PROPERTY_POST_TOPIC_FORMAT, productCode, "+", "+"), 2);
					subscribe(PREFIX_SHARE_TOPIC + clientConfig.getUsername() + String.format(THING_SERVICE_CALL_REPLY_TOPIC_FORMAT, productCode, "+", "+"), 2);
					subscribe(PREFIX_SHARE_TOPIC + clientConfig.getUsername() + String.format(THING_EVENT_POST_TOPIC_FORMAT, productCode, "+", "+"), 2);

					subscribe(String.format(INTERNAL_TOPIC_MESSAGE_MODEL_CHANGED, productCode), 2);

					queryMessageModels(productCode);
				}
			}

			@Override
			public void onFailure(Throwable cause) {
				log.error("查询{}绑定产品错误", clientConfig.getUsername(), cause);
			}

			@Override
			public void onTimeout() {
				log.error("waiting for reply of message [{}] time out", INTERNAL_MESSAGE_QUERY_BOUND_PRODUCT);
			}

		});

		reply_timeout_futures.put(messageId, submitTimeoutTask4MessageReply(messageId));

		try {
			JSONObject params = new JSONObject();
			params.put("username", clientConfig.getUsername());

			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
			String curTime = LocalDateTime.now().format(formatter);

			ThingshubMessage thingshubMessage = new ThingshubMessage();
			thingshubMessage.setId(messageId);
			thingshubMessage.setClientId(clientConfig.getClientId());
			thingshubMessage.setVersion(MESSAGE_MODEL_VERSION);
			thingshubMessage.setName(INTERNAL_MESSAGE_QUERY_BOUND_PRODUCT);
			thingshubMessage.setTime(curTime);
			thingshubMessage.setParams(params);

			MqttMessage mqttMessage = new MqttMessage();
			mqttMessage.setQos(2);
			mqttMessage.setRetained(false);
			mqttMessage.setPayload(JSON.toJSONString(thingshubMessage).getBytes(StandardCharsets.UTF_8));

			IMqttToken token = mqttAsyncClient.publish(INTERNAL_TOPIC_BOUND_PRODUCT_QUERY, mqttMessage);
			token.waitForCompletion();
		} catch (MqttException e) {
			ReplyHandler<?> replyHandler = pending_reply_handlers.remove(messageId);
			if (replyHandler != null) {
				reply_timeout_futures.remove(messageId).cancel(false);
				replyExecutor.submit(() -> replyHandler.onFailure(e));
			} else {
				log.error("", e);
			}
		}
	}

	private ScheduledFuture<?> submitTimeoutTask4MessageReply(String messageId) {
		return timeoutScheduler.schedule(() -> {
			ReplyHandler<?> replyHandler = pending_reply_handlers.remove(messageId);
			if (replyHandler != null) {
				replyExecutor.submit(() -> replyHandler.onComplete());
			}
		}, defaultTimeout, TimeUnit.SECONDS);
	}

	private void queryMessageModels(String productCode) {
		String messageId = UUID.randomUUID().toString();

		pending_reply_handlers.put(messageId, new ReplyHandler<List<MessageModel>>() {

			@Override
			public Type getType() {
				return new TypeReference<List<MessageModel>>() {
				}.getType();
			}

			@Override
			public void onSuccess(List<MessageModel> data) {
				message_model_table.clear();

				for (MessageModel item : data) {
					message_model_table.put(item.getProductCode(), item.getName(), item);
				}
			}

			@Override
			public void onFailure(Throwable cause) {
				log.error("查询产品[{}]消息模型错误", productCode, cause);
			}

			@Override
			public void onTimeout() {
				log.error("waiting for reply of message [{}] time out", INTERNAL_MESSAGE_QUERY_MESSAGE_MODEL);
			}

		});

		reply_timeout_futures.put(messageId, submitTimeoutTask4MessageReply(messageId));

		try {
			JSONObject params = new JSONObject();
			params.put("username", clientConfig.getUsername());
			params.put("productCode", productCode);

			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
			String curTime = LocalDateTime.now().format(formatter);

			ThingshubMessage thingshubMessage = new ThingshubMessage();
			thingshubMessage.setId(UUID.randomUUID().toString());
			thingshubMessage.setClientId(clientConfig.getClientId());
			thingshubMessage.setVersion(MESSAGE_MODEL_VERSION);
			thingshubMessage.setName(INTERNAL_MESSAGE_QUERY_MESSAGE_MODEL);
			thingshubMessage.setTime(curTime);
			thingshubMessage.setParams(params);

			MqttMessage mqttMessage = new MqttMessage();
			mqttMessage.setQos(2);
			mqttMessage.setRetained(false);
			mqttMessage.setPayload(JSON.toJSONString(thingshubMessage).getBytes(StandardCharsets.UTF_8));

			IMqttToken token = mqttAsyncClient.publish(INTERNAL_TOPIC_MESSAGE_MODEL_QUERY, mqttMessage);
			token.waitForCompletion();
		} catch (MqttException e) {
			ReplyHandler<?> replyHandler = pending_reply_handlers.remove(messageId);
			if (replyHandler != null) {
				reply_timeout_futures.remove(messageId).cancel(false);
				replyExecutor.submit(() -> replyHandler.onFailure(e));
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

				unsubscribe(String.format(THING_PROPERTY_POST_TOPIC_FORMAT, p, "+", "+"));
				unsubscribe(String.format(THING_SERVICE_CALL_REPLY_TOPIC_FORMAT, p, "+", "+"));
				unsubscribe(String.format(THING_EVENT_POST_TOPIC_FORMAT, p, "+", "+"));

				message_model_table.row(p).clear();

				unsubscribe(String.format(INTERNAL_TOPIC_MESSAGE_MODEL_CHANGED, p));
			} catch (ThingshubException e) {
				log.error("", e);
			}
		});

		Sets.SetView<String> addedProductCodes = Sets.difference(latestProductCodeSet, product_codes);
		addedProductCodes.forEach(p -> {
			try {
				product_codes.add(p);

				subscribe(String.format(THING_PROPERTY_POST_TOPIC_FORMAT, p, "+", "+"), 2);
				subscribe(String.format(THING_SERVICE_CALL_REPLY_TOPIC_FORMAT, p, "+", "+"), 2);
				subscribe(String.format(THING_EVENT_POST_TOPIC_FORMAT, p, "+", "+"), 2);

				subscribe(String.format(INTERNAL_TOPIC_MESSAGE_MODEL_CHANGED, p), 2);
			} catch (ThingshubException e) {
				log.error("", e);
			}
		});
	}

	private void refreshMessageModel(String action, MessageModel messageModel) {
		if ("CREATED".equals(action)) {
			message_model_table.put(messageModel.getProductCode(), messageModel.getName(), messageModel);
		} else if ("UPDATED".equals(action)) {
			message_model_table.put(messageModel.getProductCode(), messageModel.getName(), messageModel);
		} else if ("REMOVED".equals(action)) {
			message_model_table.remove(messageModel.getProductCode(), messageModel.getName());
		}
	}

	private void subscribe(String topic, int qos) {
		try {
			IMqttToken token = mqttAsyncClient.subscribe(topic, qos);
			token.waitForCompletion();

			log.info("订阅错误: {}，QoS: {}", topic, qos);
		} catch (MqttException e) {
			log.error("订阅错误，主题: {}，QoS: {}", topic, qos);
			log.error("", e);
			throw new ThingshubException("订阅错误");
		}
	}

	private void unsubscribe(String topic) {
		try {
			IMqttToken token = mqttAsyncClient.unsubscribe(topic);
			token.waitForCompletion();

			log.info("取消订阅，主题: {}", topic);
		} catch (MqttException e) {
			log.error("取消订阅错误，主题: {}", topic);
			log.error("", e);
			throw new ThingshubException("取消订阅错误");
		}
	}

	/**
	 * 发送消息。将消息发送给产品的所有设备，不需要处理设备的回复消息或设备没有回复消息
	 * 
	 * @param productCode 产品编码
	 * @param msgName     消息名称
	 * @param params      消息参数
	 */
	public <T> void publish(String productCode, String msgName, Object params) {
		this.publish(productCode, "+", msgName, params, null);
	}

	/**
	 * 发送消息。将消息发送给产品的所有设备，并通过ReplyHandler处理设备的回复消息
	 * 
	 * @param productCode  产品编码
	 * @param msgName      消息名称
	 * @param params       消息参数
	 * @param replyHandler 回复处理器
	 */
	public <T> void publish(String productCode, String msgName, Object params, ReplyHandler<T> replyHandler) {
		this.publish(productCode, "+", msgName, params, replyHandler);
	}

	/**
	 * 发送消息。将消息发送给指定设备，不需要处理设备的回复消息或设备没有回复消息
	 * 
	 * @param productCode 产品编码
	 * @param sn          设备序列号
	 * @param msgName     消息名称
	 * @param params      消息参数
	 */
	public <T> void publish(String productCode, String sn, String msgName, Object params) {
		this.publish(productCode, sn, msgName, params, null);
	}

	/**
	 * 发送消息。将消息发送给指定设备，并通过ReplyHandler处理设备的回复消息
	 * 
	 * @param productCode  产品编码
	 * @param sn           设备序列号
	 * @param msgName      消息名称
	 * @param params       消息参数
	 * @param replyHandler 回复处理器
	 */
	public <T> void publish(String productCode, String sn, String msgName, Object params, ReplyHandler<T> replyHandler) {
		if (product_codes.isEmpty() || !product_codes.contains(productCode)) {
			throw new ThingshubException(String.format("未绑定产品[%s]", productCode));
		}

		MessageModel messageModel = message_model_table.get(productCode, msgName);
		if (messageModel == null) {
			throw new ThingshubException(String.format("产品[%s]消息[%s]未定义或没有权限", productCode, msgName));
		}

		List<MessageParameter> parametersInSpec = messageModel.getType().equals(REQUEST.name()) ? messageModel.getParameters() : Collections.emptyList();
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
		if (replyHandler != null) {
			pending_reply_handlers.put(messageId, replyHandler);
			reply_timeout_futures.put(messageId, submitTimeoutTask4MessageReply(messageId));
		}

		ThingshubMessage thingshubMessage = new ThingshubMessage();
		thingshubMessage.setId(messageId);
		thingshubMessage.setClientId(clientConfig.getClientId());
		thingshubMessage.setVersion(MESSAGE_MODEL_VERSION);
		thingshubMessage.setName(msgName);
		thingshubMessage.setTime(curTime);
		thingshubMessage.setParams(params);
		String msgTopic = String.format(THING_SERVICE_CALL_TOPIC_FORMAT, messageModel.getProductCode(), sn, messageModel.getName());

		MqttMessage mqttMessage = new MqttMessage();
		mqttMessage.setQos(2);
		mqttMessage.setRetained(false);
		mqttMessage.setPayload(JSON.toJSONString(thingshubMessage).getBytes(StandardCharsets.UTF_8));

		try {
			IMqttToken token = mqttAsyncClient.publish(msgTopic, mqttMessage);
			token.waitForCompletion();
		} catch (MqttException e) {
			ReplyHandler<?> theReplyHandler = pending_reply_handlers.remove(messageId);
			if (theReplyHandler != null) {
				reply_timeout_futures.remove(messageId).cancel(false);
				replyExecutor.submit(() -> theReplyHandler.onFailure(e));
			} else {
				log.error("发送消息错误，message name：{}，message id：{}，error：", messageId, msgName, e);
				throw new ThingshubException("发送消息错误：" + e.getMessage());
			}
		}
	}

	private void validateMessageParameter(Object paramObj, List<MessageParameter> parametersInSpec) {
		JSONObject paramObjInJSON = JSONObject.from(paramObj);
		Set<String> paramIdsInPayload = paramObjInJSON.keySet();
		Set<String> paramIdsInSpec = Sets.newHashSet();

		for (MessageParameter mp : parametersInSpec) {
			paramIdsInSpec.add(mp.getIdentifier());

			DataType parameterDataType = DataType.from(mp.getDataType());
			if (parameterDataType != null) {
				Object parameterValue = paramObjInJSON.get(mp.getIdentifier());
				if (parameterValue != null) {
					ValidateResult vResult = parameterDataType.validate(parameterValue);
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
	 * @param msgId       消息ID
	 * @param msgName     消息名称
	 * @param data        回复内容
	 */
	public void reply(String productCode, String sn, String msgId, String msgName, Object data) {
		if (product_codes.isEmpty() || !product_codes.contains(productCode)) {
			throw new ThingshubException(String.format("未绑定产品[%s]", productCode));
		}

		MessageModel messageModel = message_model_table.get(productCode, msgName);
		if (messageModel == null) {
			throw new ThingshubException(String.format("产品[%s]消息名称[%s]未定义", productCode, msgName));
		}

		List<MessageParameter> parametersInSpec = messageModel.getType().equals(REPLY.name()) ? messageModel.getParameters() : Collections.emptyList();
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
		thingshubMessage.setClientId(clientConfig.getClientId());
		thingshubMessage.setVersion(MESSAGE_MODEL_VERSION);
		thingshubMessage.setName(msgName);
		thingshubMessage.setTime(curTime);
		thingshubMessage.setCode(MessageResult.SUCCESS.code());
		thingshubMessage.setMessage(MessageResult.SUCCESS.desc());
		thingshubMessage.setData(data);

		String msgTopic = switch (ThingModelType.of(messageModel.getCat())) {
		case PROPERTY -> String.format(THING_PROPERTY_POST_REPLY_TOPIC_FORMAT, messageModel.getProductCode(), sn, messageModel.getName());
		case EVENT -> String.format(THING_EVENT_POST_REPLY_TOPIC_FORMAT, messageModel.getProductCode(), sn, messageModel.getName());
		default -> throw new ThingshubException("消息类别错误: " + messageModel.getCat());
		};

		MqttMessage mqttMessage = new MqttMessage();
		mqttMessage.setQos(2);
		mqttMessage.setRetained(false);
		mqttMessage.setPayload(JSON.toJSONString(thingshubMessage).getBytes(StandardCharsets.UTF_8));

		try {
			IMqttToken token = mqttAsyncClient.publish(msgTopic, mqttMessage);
			token.waitForCompletion();
		} catch (MqttException e) {
			log.error("发送消息错误，message name：{}，message id：{}，error：", msgId, msgName, e);
			throw new ThingshubException("发送消息错误：" + e.getMessage());
		}
	}

	/**
	 * 回复错误消息。将消息发送给指定设备
	 * 
	 * @param productCode 产品编码
	 * @param sn          设备序列号
	 * @param msgId       消息ID
	 * @param msgName     消息名称
	 * @param errCode     错误代码
	 * @param errMsg      错误信息
	 */
	public void replyWithError(String productCode, String sn, String msgId, String msgName, Integer errCode, String errMsg) {
		if (product_codes.isEmpty() || !product_codes.contains(productCode)) {
			throw new ThingshubException(String.format("未绑定产品[%s]", productCode));
		}

		MessageModel messageModel = message_model_table.get(productCode, msgName);
		if (messageModel == null) {
			throw new ThingshubException(String.format("产品[%s]消息名称[%s]未定义", productCode, msgName));
		}

		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
		String curTime = LocalDateTime.now().format(formatter);

		ThingshubMessage thingshubMessage = new ThingshubMessage();
		thingshubMessage.setId(msgId);
		thingshubMessage.setClientId(clientConfig.getClientId());
		thingshubMessage.setVersion(MESSAGE_MODEL_VERSION);
		thingshubMessage.setName(msgName);
		thingshubMessage.setTime(curTime);
		thingshubMessage.setCode(errCode);
		thingshubMessage.setMessage(errMsg);

		String msgTopic = switch (ThingModelType.of(messageModel.getCat())) {
		case PROPERTY -> String.format(THING_PROPERTY_POST_REPLY_TOPIC_FORMAT, messageModel.getProductCode(), sn, messageModel.getName());
		case EVENT -> String.format(THING_EVENT_POST_REPLY_TOPIC_FORMAT, messageModel.getProductCode(), sn, messageModel.getName());
		default -> throw new ThingshubException(String.format("与消息模型中的类别%s不匹配", messageModel.getCat()));
		};

		MqttMessage mqttMessage = new MqttMessage();
		mqttMessage.setQos(2);
		mqttMessage.setRetained(false);
		mqttMessage.setPayload(JSON.toJSONString(thingshubMessage).getBytes(StandardCharsets.UTF_8));

		try {
			IMqttToken token = mqttAsyncClient.publish(msgTopic, mqttMessage);
			token.waitForCompletion();
		} catch (MqttException e) {
			log.error("发送消息错误，message name：{}，message id：{}，error：", msgId, msgName, e);
			throw new ThingshubException("发送消息错误：" + e.getMessage());
		}
	}

	private void disconnect() {
		if (mqttAsyncClient != null && mqttAsyncClient.isConnected()) {
			try {
				IMqttToken token = mqttAsyncClient.disconnect();
				token.waitForCompletion();
			} catch (MqttException e) {
				log.error("Disconnecting failed: ", e);
			}
		}
		mqttAsyncClient = null;
	}

	/**
	 * 获取设备提供的服务
	 * 
	 * @param productCode 产品编码
	 * @return
	 */
	public List<ThingServiceItem> getServicesOfProduct(String productCode) {
		Map<String, MessageModel> messageModels = message_model_table.row(productCode);
		if (messageModels == null) {
			return Collections.emptyList();
		}

		return messageModels.values().stream().filter(m -> m.getCat().equalsIgnoreCase(SERVICE.name()) && m.getType().equalsIgnoreCase(REQUEST.name())).map(m -> {
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
	 * @param queryCriterions 查询条件
	 * @return
	 */
	public Page<DeviceInfo> queryDevice(DeviceQueryCriterions queryCriterions) {
		String messageId = UUID.randomUUID().toString();

		CompletableFuture<Page<DeviceInfo>> future = new CompletableFuture<>();
		pending_reply_handlers.put(messageId, new ReplyHandler<Page<DeviceInfo>>() {

			@Override
			public Type getType() {
				return new TypeReference<Page<DeviceInfo>>() {
				}.getType();
			}

			@Override
			public void onSuccess(Page<DeviceInfo> data) {
				future.complete(data);
			}

			@Override
			public void onFailure(Throwable cause) {
				log.error("查询设备错误", cause);
				future.completeExceptionally(cause);
			}

			@Override
			public void onTimeout() {
				log.error("waiting for reply of message [{}] time out", INTERNAL_MESSAGE_QUERY_DEVICE);
				future.completeExceptionally(new ThingshubException("device query response timeout after " + defaultTimeout + " seconds"));
			}
		});
		reply_timeout_futures.put(messageId, submitTimeoutTask4MessageReply(messageId));

		try {
			JSONObject params = new JSONObject();
			params.put("username", clientConfig.getUsername());
			params.put("queryCriterions", queryCriterions);

			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
			String curTime = LocalDate.now().format(formatter);

			ThingshubMessage thingshubMessage = new ThingshubMessage();
			thingshubMessage.setId(UUID.randomUUID().toString());
			thingshubMessage.setClientId(clientConfig.getClientId());
			thingshubMessage.setVersion(MESSAGE_MODEL_VERSION);
			thingshubMessage.setName(INTERNAL_MESSAGE_QUERY_DEVICE);
			thingshubMessage.setTime(curTime);
			thingshubMessage.setParams(params);

			MqttMessage mqttMessage = new MqttMessage();
			mqttMessage.setQos(2);
			mqttMessage.setRetained(false);
			mqttMessage.setPayload(JSON.toJSONString(thingshubMessage).getBytes(StandardCharsets.UTF_8));

			IMqttToken token = mqttAsyncClient.publish(INTERNAL_TOPIC_DEVICE_QUERY, mqttMessage);
			token.waitForCompletion();

			return future.get();
		} catch (MqttException | InterruptedException | ExecutionException e) {
			ReplyHandler<?> replyHandler = pending_reply_handlers.remove(messageId);
			if (replyHandler != null) {
				reply_timeout_futures.remove(messageId).cancel(false);
				replyExecutor.submit(() -> replyHandler.onFailure(e));
			} else {
				log.error("", e);
			}

			throw new ThingshubException("查询设备错误：" + e.getMessage());
		}
	}

	/**
	 * 获取设备基本信息
	 * 
	 * @param productCode 产品编码
	 * @param sn          设备序列号
	 * @return
	 */
	public DeviceInfo getDeviceInfo(String productCode, String sn) {
		String messageId = UUID.randomUUID().toString();

		CompletableFuture<DeviceInfo> future = new CompletableFuture<>();
		pending_reply_handlers.put(messageId, new ReplyHandler<DeviceInfo>() {

			@Override
			public Type getType() {
				return new TypeReference<DeviceInfo>() {
				}.getType();
			}

			@Override
			public void onSuccess(DeviceInfo data) {
				future.complete(data);
			}

			@Override
			public void onFailure(Throwable cause) {
				log.error("获取设备信息错误", cause);
				future.completeExceptionally(cause);
			}

			@Override
			public void onTimeout() {
				log.error("waiting for reply of message [{}] time out", INTERNAL_MESSAGE_DEVICE_INFO);
				future.completeExceptionally(new ThingshubException("get device info response timeout after " + defaultTimeout + " seconds"));
			}
		});
		reply_timeout_futures.put(messageId, submitTimeoutTask4MessageReply(messageId));

		try {
			JSONObject params = new JSONObject();
			params.put("username", clientConfig.getUsername());
			params.put("productCode", productCode);
			params.put("sn", sn);

			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
			String curTime = LocalDateTime.now().format(formatter);

			ThingshubMessage thingshubMessage = new ThingshubMessage();
			thingshubMessage.setId(UUID.randomUUID().toString());
			thingshubMessage.setClientId(clientConfig.getClientId());
			thingshubMessage.setVersion(MESSAGE_MODEL_VERSION);
			thingshubMessage.setName(INTERNAL_MESSAGE_DEVICE_INFO);
			thingshubMessage.setTime(curTime);
			thingshubMessage.setParams(params);

			MqttMessage mqttMessage = new MqttMessage();
			mqttMessage.setQos(2);
			mqttMessage.setRetained(false);
			mqttMessage.setPayload(JSON.toJSONString(thingshubMessage).getBytes(StandardCharsets.UTF_8));

			IMqttToken token = mqttAsyncClient.publish(INTERNAL_TOPIC_DEVICE_INFO, mqttMessage);
			token.waitForCompletion();

			return future.get();
		} catch (MqttException | InterruptedException | ExecutionException e) {
			ReplyHandler<?> replyHandler = pending_reply_handlers.remove(messageId);
			if (replyHandler != null) {
				reply_timeout_futures.remove(messageId).cancel(false);
				replyExecutor.submit(() -> replyHandler.onFailure(e));
			} else {
				log.error("", e);
			}

			throw new ThingshubException("获取设备信息错误：" + e.getMessage());
		}
	}

	/**
	 * 禁用设备
	 * 
	 * @param productCode 产品编码
	 * @param sn          设备序列号
	 */
	public void disableDevice(String productCode, String sn) {
		String messageId = UUID.randomUUID().toString();

		CompletableFuture<Void> future = new CompletableFuture<>();
		pending_reply_handlers.put(messageId, new ReplyHandler<Void>() {

			@Override
			public Type getType() {
				return new TypeReference<Void>() {
				}.getType();
			}

			@Override
			public void onSuccess(Void data) {
				future.complete(data);
			}

			@Override
			public void onFailure(Throwable cause) {
				log.error("禁用设备错误", cause);
				future.completeExceptionally(cause);
			}

			@Override
			public void onTimeout() {
				log.error("waiting for reply of message [{}] time out", INTERNAL_MESSAGE_DEVICE_INFO);
				future.completeExceptionally(new ThingshubException("disable device response timeout after " + defaultTimeout + " seconds"));
			}
		});
		reply_timeout_futures.put(messageId, submitTimeoutTask4MessageReply(messageId));

		try {
			JSONObject params = new JSONObject();
			params.put("productCode", productCode);
			params.put("sn", sn);

			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
			String curTime = LocalDateTime.now().format(formatter);

			ThingshubMessage thingshubMessage = new ThingshubMessage();
			thingshubMessage.setId(UUID.randomUUID().toString());
			thingshubMessage.setClientId(clientConfig.getClientId());
			thingshubMessage.setVersion(MESSAGE_MODEL_VERSION);
			thingshubMessage.setName(INTERNAL_MESSAGE_DISABLE_DEVICE);
			thingshubMessage.setTime(curTime);
			thingshubMessage.setParams(params);

			MqttMessage mqttMessage = new MqttMessage();
			mqttMessage.setQos(2);
			mqttMessage.setRetained(false);
			mqttMessage.setPayload(JSON.toJSONString(thingshubMessage).getBytes(StandardCharsets.UTF_8));

			IMqttToken token = mqttAsyncClient.publish(INTERNAL_TOPIC_DEVICE_DISABLE, mqttMessage);
			token.waitForCompletion();

			future.get();
		} catch (MqttException | InterruptedException | ExecutionException e) {
			ReplyHandler<?> replyHandler = pending_reply_handlers.remove(messageId);
			if (replyHandler != null) {
				reply_timeout_futures.remove(messageId).cancel(false);
				replyExecutor.submit(() -> replyHandler.onFailure(e));
			} else {
				log.error("", e);
			}

			throw new ThingshubException("禁用设备错误：" + e.getMessage());
		}
	}

	/**
	 * 启用设备
	 * 
	 * @param productCode 产品编码
	 * @param sn          设备序列号
	 */
	public void enableDevice(String productCode, String sn) {
		String messageId = UUID.randomUUID().toString();

		CompletableFuture<Void> future = new CompletableFuture<>();
		pending_reply_handlers.put(messageId, new ReplyHandler<Void>() {

			@Override
			public Type getType() {
				return new TypeReference<Void>() {
				}.getType();
			}

			@Override
			public void onSuccess(Void data) {
				future.complete(data);
			}

			@Override
			public void onFailure(Throwable cause) {
				log.error("启用设备错误", cause);
				future.completeExceptionally(cause);
			}

			@Override
			public void onTimeout() {
				log.error("waiting for reply of message [{}] time out", INTERNAL_MESSAGE_DEVICE_INFO);
				future.completeExceptionally(new ThingshubException("enable device response timeout after " + defaultTimeout + " seconds"));
			}
		});
		reply_timeout_futures.put(messageId, submitTimeoutTask4MessageReply(messageId));

		try {
			JSONObject params = new JSONObject();
			params.put("productCode", productCode);
			params.put("sn", sn);

			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
			String curTime = LocalDateTime.now().format(formatter);

			ThingshubMessage thingshubMessage = new ThingshubMessage();
			thingshubMessage.setId(UUID.randomUUID().toString());
			thingshubMessage.setClientId(clientConfig.getClientId());
			thingshubMessage.setVersion(MESSAGE_MODEL_VERSION);
			thingshubMessage.setName(INTERNAL_MESSAGE_ENABLE_DEVICE);
			thingshubMessage.setTime(curTime);
			thingshubMessage.setParams(params);

			MqttMessage mqttMessage = new MqttMessage();
			mqttMessage.setQos(2);
			mqttMessage.setRetained(false);
			mqttMessage.setPayload(JSON.toJSONString(thingshubMessage).getBytes(StandardCharsets.UTF_8));

			IMqttToken token = mqttAsyncClient.publish(INTERNAL_TOPIC_DEVICE_ENABLE, mqttMessage);
			token.waitForCompletion();

			future.get();
		} catch (MqttException | InterruptedException | ExecutionException e) {
			ReplyHandler<?> replyHandler = pending_reply_handlers.remove(messageId);
			if (replyHandler != null) {
				reply_timeout_futures.remove(messageId).cancel(false);
				replyExecutor.submit(() -> replyHandler.onFailure(e));
			} else {
				log.error("", e);
			}

			throw new ThingshubException("启用设备错误：" + e.getMessage());
		}
	}

	public void shutdown() {
		if (mqttAsyncClient != null && mqttAsyncClient.isConnected()) {
			unsubscribe(String.format(INTERNAL_TOPIC_BOUND_PRODUCT_CHANGED, clientConfig.getUsername()));
			unsubscribe(String.format(INTERNAL_TOPIC_BOUND_PRODUCT_QUERY_REPLY, clientConfig.getUsername(), clientConfig.getClientId()));
			unsubscribe(String.format(INTERNAL_TOPIC_MESSAGE_MODEL_QUERY_REPLY, clientConfig.getUsername(), clientConfig.getClientId()));
			unsubscribe(String.format(INTERNAL_TOPIC_DEVICE_QUERY_REPLY, clientConfig.getUsername(), clientConfig.getClientId()));
			unsubscribe(String.format(INTERNAL_TOPIC_DEVICE_INFO_REPLY, clientConfig.getUsername(), clientConfig.getClientId()));
			unsubscribe(String.format(INTERNAL_TOPIC_DEVICE_DISABLE_REPLY, clientConfig.getUsername(), clientConfig.getClientId()));
			unsubscribe(String.format(INTERNAL_TOPIC_DEVICE_ENABLE_REPLY, clientConfig.getUsername(), clientConfig.getClientId()));
		}

		product_codes.forEach(p -> {
			if (mqttAsyncClient != null && mqttAsyncClient.isConnected()) {
				try {
					unsubscribe(String.format(INTERNAL_TOPIC_MESSAGE_MODEL_CHANGED, clientConfig.getUsername(), p));

					unsubscribe(String.format(THING_PROPERTY_POST_TOPIC_FORMAT, p, "+", "+"));
					unsubscribe(String.format(THING_SERVICE_CALL_REPLY_TOPIC_FORMAT, p, "+", "+"));
					unsubscribe(String.format(THING_EVENT_POST_TOPIC_FORMAT, p, "+", "+"));
				} catch (ThingshubException e) {
					log.error("", e);
				}
			}
		});

		this.disconnect();
	}

}
