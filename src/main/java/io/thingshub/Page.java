package io.thingshub;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

public class Page<T> implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * 查询数据列表
	 */
	protected List<T> records = Collections.emptyList();

	/**
	 * 总数
	 */
	protected int total = 0;

	/**
	 * 当前页的数据条数
	 */
	protected int size = 0;

	/**
	 * 当前页
	 */
	protected int current = 1;

	public Page() {
	}

	public Page(int current, int size) {
		this(current, size, 0, null);
	}

	public Page(int current, int size, int total) {
		this(current, size, total, null);
	}

	public Page(int current, int size, int total, List<T> records) {
		if (current > 1) {
			this.current = current;
		}
		this.size = size;
		this.total = total;
		if (records != null) {
			this.records = records;
		}
	}

	public boolean hasPrevious() {
		return this.current > 1;
	}

	public boolean hasNext() {
		return this.current < this.getPages();
	}

	public List<T> getRecords() {
		return this.records;
	}

	public Page<T> setRecords(List<T> records) {
		this.records = records;
		return this;
	}

	public int getTotal() {
		return this.total;
	}

	public Page<T> setTotal(int total) {
		this.total = total;
		return this;
	}

	public int getSize() {
		return this.size;
	}

	public Page<T> setSize(int size) {
		this.size = size;
		return this;
	}

	public int getCurrent() {
		return this.current;
	}

	public Page<T> setCurrent(int current) {
		this.current = current;
		return this;
	}

	public int getPages() {
		if (getSize() == 0) {
			return 0;
		}
		int pages = getTotal() / getSize();
		if (getTotal() % getSize() != 0) {
			pages++;
		}

		return pages;
	}

	public static <T> Page<T> of(int current, int size, int total, List<T> records) {
		return new Page<>(current, size, total, records);
	}

	public static <T> Page<T> of(int current, int size, int total) {
		return new Page<>(current, size, total);
	}

	public static <T> Page<T> of(int current, int size) {
		return of(current, size, 0);
	}

}
