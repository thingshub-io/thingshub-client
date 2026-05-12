package io.thingshub.client;

import java.io.Serializable;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

/**
 * <p>
 * 设备主发消息
 * </p>
 *
 * @author albert pi
 * @since 1.0.0
 */

@Getter
@Builder
@ToString
public class Message implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * 消息ID
	 */
	private String id;

	/**
	 * 消息名称
	 */
	private String name;

	/**
	 * 设备序列号
	 */
	private String sn;

	/**
	 * 消息参数
	 */
	private Object params;

}