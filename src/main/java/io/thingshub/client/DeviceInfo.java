package io.thingshub.client;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;

import lombok.Data;

/**
 * <p>
 * 设备信息
 * </p>
 *
 * @author albert pi
 * @since 1.0.0
 */
@Data
public class DeviceInfo implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * ID
	 */
	private Long id;

	/**
	 * 设备编号
	 */
	private String sn;

	/**
	 * 分组编号
	 */
	private Long groupId;

	/**
	 * 分组名称
	 */
	private String groupName;

	/**
	 * 产品编码
	 */
	private String productCode;

	/**
	 * 产品名称
	 */
	private String productName;

	/**
	 * 协议版本
	 */
	private String protocolVersion;

	/**
	 * 设备所在地区行政编码
	 */
	private String region;

	/**
	 * 设备当前的详细地址
	 */
	private String address;

	/**
	 * 设备当前的纬度
	 */
	private BigDecimal lat;

	/**
	 * 设备当前的经度
	 */
	private BigDecimal lng;

	/**
	 * 设备二维码
	 */
	private String qrCode;

	/**
	 * 连接状态。0-离线；1-在线；
	 */
	private Integer connectState;

	/**
	 * 故障状态。0-正常；1-故障；
	 */
	private Integer faultState;

	/**
	 * 设备最后上报时间
	 */
	private Date reportTime;

	/**
	 * 备注
	 */
	private String remark;

	/**
	 * 状态。0-正常；1-禁用；
	 */
	private Integer status;

	/**
	 * 创建时间
	 */
	private Date createTime;

	/**
	 * 创建者账号名称
	 */
	private String createBy;

}