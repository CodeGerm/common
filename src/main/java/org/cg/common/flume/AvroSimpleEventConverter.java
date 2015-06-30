/**
 * 
 */
package org.cg.common.flume;

import java.util.Map;

import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.cg.common.avro.BeanConvertor;
import org.cg.common.avro.BeanGenerator;


/**
 * Convert avro object from/to flume simple event 
 * 
 * @author yanlinwang
 *
 */
public class AvroSimpleEventConverter <A>{
	
	public static final String[] EXCLUDE = new String[] { "schema" };
	
	BeanConvertor bean2map;
	BeanGenerator map2bean;
	
	public AvroSimpleEventConverter(Class<A> avroTypeClass ) {
		bean2map = new BeanConvertor(EXCLUDE);
		map2bean = new BeanGenerator(avroTypeClass);
	}

	public Event objectToSimpleEvent(A avroObject){
		SimpleEvent flowEvent = new SimpleEvent();
		Map target  = bean2map.transform(avroObject);
        flowEvent.getHeaders().putAll(target); 
        return flowEvent;
	}
	
	public A simpleEventToObject (Event flowEvent){
		return (A) map2bean.transform(flowEvent.getHeaders());
	}

}
