package io.thingshub.meta;

import java.text.ParseException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.Map;

import org.apache.commons.lang3.time.DateUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DateType extends DataType {

	private ZoneId zoneId = ZoneId.systemDefault();

	public static DateType fromSpecs(Map<String, Object> specs) {
		return new DateType();
	}

	@Override
	public String getName() {
		return "date";
	}

	@Override
	public String getTitle() {
		return "日期时间";
	}

	@Override
	public ValidateResult validate(Object value) {
		Date date = null;
		if (value instanceof Instant) {
			date = Date.from(((Instant) value));
		} else if (value instanceof LocalDateTime) {
			date = Date.from(((LocalDateTime) value).atZone(zoneId).toInstant());
		} else if (value instanceof Date) {
			date = ((Date) value);
		} else if (value instanceof Number) {
			date = new Date(((Number) value).longValue());
		}
		if (value instanceof String) {
			try {
				date = DateUtils.parseDate((String) value, "yyyy-MM-dd HH:mm:ss");
			} catch (ParseException e) {
				log.error("", e);

				throw new RuntimeException(e.getMessage());
			}
		}

		if (date == null) {
			return ValidateResult.builder().success(false).error("非法的日期时间值[" + value + "]").build();
		}

		return ValidateResult.builder().success(true).build();
	}

}
