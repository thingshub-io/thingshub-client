package io.thingshub.client;

import java.io.Serializable;
import java.util.List;

import io.thingshub.MessageParameter;
import lombok.Data;

/**
 * <p>
 * 设备物模型中服务的定义
 * </p>
 *
 * @author albert pi
 * @since 1.0.0
 */
@Data
public class ThingServiceItem implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * 名称
	 */
	private String name;

	/**
	 * 标题
	 */
	private String title;

	/**
	 * 参数定义
	 */
	private List<MessageParameter> parameters;

}