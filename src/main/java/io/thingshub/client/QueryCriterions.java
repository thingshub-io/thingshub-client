package io.thingshub.client;

import java.util.Map;

import com.alibaba.fastjson2.JSON;

import lombok.Data;

/**
 * <p>
 * 查询参数
 * </p>
 *
 * @author albert pi
 * @since 1.0.0
 */

@Data
public abstract class QueryCriterions {

	/**
	 * 分页号
	 */
	protected Integer page = 1;

	/**
	 * 返回记录数量
	 */
	protected Integer size = 10;

	public Map<String, Object> toMap() {
		return JSON.parseObject(JSON.toJSONString(this));
	}

}