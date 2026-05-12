package io.thingshub.client;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class ClientConfig {

	/**
	 * Thingshub MQTT服务器地址
	 */
	private String host;

	/**
	 * Thingshub MQTT服务器port
	 */
	private Integer port;

	/**
	 * Thingshub MQTT服务器user name
	 */
	private String username;

	/**
	 * Thingshub MQTT服务器password
	 */
	private String password;

	/**
	 * 客户端client ID
	 */
	private String clientId;

	/**
	 * 客户端clean start(clean session)属性
	 */
	@Builder.Default
	private Boolean cleanStart = true;

	/**
	 * 客户端连接超时（秒）
	 */
	@Builder.Default
	private Integer connectTimeout = 30;

	/**
	 * 客户端重连最大延迟时间（秒）
	 */
	@Builder.Default
	private Integer maxReconnectDelay = 128;

	/**
	 * 客户端保活时间（秒）
	 */
	@Builder.Default
	private Integer keepAlive = 60;

}
