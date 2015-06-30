/**
 * 
 */
package org.cg.common.avro;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.cg.common.avro.UTF8Serializer;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.AnnotationIntrospector;
import org.codehaus.jackson.map.MapperConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.PropertyNamingStrategy;
import org.codehaus.jackson.map.introspect.AnnotatedClass;
import org.codehaus.jackson.map.introspect.AnnotatedField;
import org.codehaus.jackson.map.introspect.AnnotatedMethod;
import org.codehaus.jackson.map.introspect.JacksonAnnotationIntrospector;
import org.codehaus.jackson.map.module.SimpleModule;
import org.codehaus.jackson.map.ser.BeanPropertyFilter;
import org.codehaus.jackson.map.ser.FilterProvider;
import org.codehaus.jackson.map.ser.impl.SimpleBeanPropertyFilter;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;




/**
 * convert java bean to map 
 * 
 * @author yanlinwang
 *
 */
public class BeanConvertor  {

		private final ObjectMapper objectMapper = new ObjectMapper();

		private volatile boolean shouldFlattenKeys = true;
		private volatile boolean shouldNormalizeString = true;
		private volatile HashSet<String> mapNameSet = new HashSet<String>();	
		private volatile Object registered;
		

		public BeanConvertor (final String... exclusions){
			AnnotationIntrospector intr = new JacksonAnnotationIntrospector() {
				@Override
				public Object findFilterId(AnnotatedClass ac) {
					// Let's default to current behavior if annotation is found:
					Object id = super.findFilterId(ac);
					// but use simple class name if not
					if (id == null) {
						id = ac.getName();
					}
					return id;
				};
			};
			objectMapper.getSerializationConfig().setAnnotationIntrospector(intr);
			objectMapper.getDeserializationConfig().setAnnotationIntrospector(intr);
			objectMapper.setFilters(new FilterProvider() {
				@Override
				public BeanPropertyFilter findFilter(Object filterId) {
					return SimpleBeanPropertyFilter.serializeAllExcept(exclusions);
				}
			});
			objectMapper.setPropertyNamingStrategy(new PropertyNamingStrategy(){

				@Override
				public String nameForField(MapperConfig<?> config,
						AnnotatedField field, String defaultName) {
					if (registered==null && field.getDeclaringClass().isAssignableFrom(Map.class))
						mapNameSet.add(defaultName);
					return super.nameForField(config, field, defaultName);
				}

				@Override
				public String nameForGetterMethod(MapperConfig<?> config,
						AnnotatedMethod method, String defaultName) {
					if (registered==null && method.getRawType().isAssignableFrom(Map.class))
						mapNameSet.add(defaultName);
					return super.nameForGetterMethod(config, method, defaultName);
				}

				@Override
				public String nameForSetterMethod(MapperConfig<?> config,
						AnnotatedMethod method, String defaultName) {
					if (method.getDeclaringClass().isAssignableFrom(Map.class))
						mapNameSet.add(defaultName);
					return super.nameForSetterMethod(config, method, defaultName);
				}
				
			});
			SimpleModule testModule = new SimpleModule("MyModule", new Version(1, 0, 0, null));
			testModule.addSerializer(new UTF8Serializer()); // assuming serializer declares correct class to bind to
			objectMapper.registerModule(testModule);
		}
		
		public void setShouldFlattenKeys(boolean shouldFlattenKeys) {
			this.shouldFlattenKeys = shouldFlattenKeys;
		}
		
		

		public boolean isShouldFlattenKeys() {
			return shouldFlattenKeys;
		}

		public boolean isShouldNormalizeString() {
			return shouldNormalizeString;
		}

		public void setShouldNormalizeString(boolean shouldNormalizeString) {
			this.shouldNormalizeString = shouldNormalizeString;
		}

		@SuppressWarnings("unchecked")
		public Map<String, Object> transform(Object payload)  {
			Map<String,Object> result = null;
			//only first time scan we need to setup mapNameSet 
			if (registered==null){
				synchronized (BeanConvertor.class) {
					if (registered==null){
						result = (Map<String,Object>)this.objectMapper.convertValue(payload, Map.class);
						registered = new Object(); 
					} else {
						result = (Map<String,Object>)this.objectMapper.convertValue(payload, Map.class);
					}
				}
			} else {				
				result = (Map<String,Object>)this.objectMapper.convertValue(payload, Map.class);
			}
			
			if (this.shouldFlattenKeys) {
				result = this.flattenMap(result);
			}
			return result;
		}

		private Map<String, Object> flattenMap(Map<String,Object> result){
			Map<String,Object> resultMap = new HashMap<String, Object>();
			this.doFlatten("", result, resultMap);
			return resultMap;
		}

		private void doFlatten(String propertyPrefix, Map<String,Object> inputMap, Map<String,Object> resultMap){
			if (StringUtils.hasText(propertyPrefix)) {
				propertyPrefix = propertyPrefix + ".";
			}
			for (String key : inputMap.keySet()) {
				Object value = inputMap.get(key);
				this.doProcessElement(propertyPrefix + key, value, resultMap);
			}
		}

		private void doFlattenMap(String propertyPrefix, Map<String,Object> inputMap, Map<String,Object> resultMap){
			if (StringUtils.hasText(propertyPrefix)) {
				propertyPrefix = propertyPrefix + "[";
			}
			for (String key : inputMap.keySet()) {
				Object value = inputMap.get(key);
				this.doProcessElement(propertyPrefix + key + "]", value, resultMap);
			}
		}
		
		
		private void doProcessCollection(String propertyPrefix,  Collection<?> list, Map<String, Object> resultMap) {
			int counter = 0;
			for (Object element : list) {
				this.doProcessElement(propertyPrefix + "[" + counter + "]", element, resultMap);
				counter ++;
			}
		}

		@SuppressWarnings("unchecked")
		private void doProcessElement(String propertyPrefix, Object element, Map<String, Object> resultMap) {
			if (element instanceof Map) {
				int start = propertyPrefix.lastIndexOf(".");
				String key = propertyPrefix.substring(start +1);
				if (mapNameSet.contains(key))
					this.doFlattenMap(propertyPrefix, (Map<String, Object>) element, resultMap);
				else
					this.doFlatten(propertyPrefix, (Map<String, Object>) element, resultMap);
			}
			else if (element instanceof Collection) {
				this.doProcessCollection(propertyPrefix, (Collection<?>) element, resultMap);
			}
			else if (element != null && element.getClass().isArray()) {
				Collection<?> collection =  CollectionUtils.arrayToList(element);
				this.doProcessCollection(propertyPrefix, collection, resultMap);
			}
			else {
				if (shouldNormalizeString ) {
					if (element != null){
						element = element.toString();
						resultMap.put(propertyPrefix, element);
					}
				} else {
					resultMap.put(propertyPrefix, element);
				}
			}
		}

	
}
