package io.thingshub.client;

import java.util.Map;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component
public final class SpringUtils implements BeanFactoryPostProcessor, ApplicationContextAware {

	private static ConfigurableListableBeanFactory beanFactory;

	private static ApplicationContext applicationContext;

	@Override
	public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) {
		SpringUtils.beanFactory = beanFactory;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) {
		SpringUtils.applicationContext = applicationContext;
	}

	public static ListableBeanFactory getBeanFactory() {
		return null == beanFactory ? applicationContext : beanFactory;
	}

	/**
	 * 获取名称为name的bean
	 * 
	 * @param <T>
	 * @param name
	 * @return
	 * @throws BeansException
	 */
	@SuppressWarnings("unchecked")
	public static <T> T getBean(String name) throws BeansException {
		return (T) getBeanFactory().getBean(name);
	}

	/**
	 * 获取类型为T的bean
	 * 
	 * @param <T>
	 * @param clazz
	 * @return
	 * @throws BeansException
	 */
	public static <T> T getBean(Class<T> clazz) throws BeansException {
		return getBeanFactory().getBean(clazz);
	}

	/**
	 * 获取类型为T的bean
	 * 
	 * @param <T>
	 * @param clazz
	 * @return
	 * @throws BeansException
	 */
	public static <T> Map<String, T> getBeans(Class<T> clazz) throws BeansException {
		return getBeanFactory().getBeansOfType(clazz);
	}

}