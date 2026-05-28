package io.thingshub.client;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * <p>
 * 设备查询参数
 * </p>
 *
 * @author albert pi
 * @since 1.0.0
 */

@Data
@EqualsAndHashCode(callSuper = false)
public class DeviceQueryCriterions extends QueryCriterions {

	/**
	 * 所属租户ID
	 */
	private String tenantId;

	/**
	 * 所属客户ID
	 */
	private String customerId;

	/**
	 * 设备所在地区行政编码
	 */
	private String region;

	/**
	 * 分组
	 */
	private String group;

	/**
	 * 产品编码
	 */
	private String productCode;

	/**
	 * 设备序列号
	 */
	private String sn;

}