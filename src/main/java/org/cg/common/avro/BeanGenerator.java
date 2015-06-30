/**
 * 
 */
package org.cg.common.avro;

import java.util.Map;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.MutablePropertyValues;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.integration.transformer.MessageTransformationException;
import org.springframework.validation.DataBinder;

/**
 * Convert from map back to java bean
 * 
 * @author yanlinwang
 * 
 */
public class BeanGenerator {
	private Class<?> targetClass;

	/**
	 * @param targetClass
	 */
	public BeanGenerator(Class<?> targetClass) {
		try {
			this.targetClass = targetClass;
		} catch (Exception e) {
			throw new MessageTransformationException(
					"can not create instance of " + targetClass, e);
		}
	}

	@SuppressWarnings("unchecked")
	public Object transform(Map<?, ?> payload)  {
		Object target = null;
		if (targetClass != null) {
			target = BeanUtils.instantiate(targetClass);
		} else {
			throw new MessageTransformationException(
					"'targetClass or target 'beanName' must be specified");
		}
		DataBinder binder = new DataBinder(target);
		binder.setConversionService(new DefaultConversionService());
		binder.bind(new MutablePropertyValues((Map) payload));
		return target;
	}
}
