/**
 * 
 */
package org.cg.common.avro;

import java.io.IOException;

import org.apache.avro.util.Utf8;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.map.TypeSerializer;
import org.codehaus.jackson.map.ser.ScalarSerializerBase;


/**
 * @author yanlinwang
 *
 */
public class UTF8Serializer extends ScalarSerializerBase<Utf8> {

	public UTF8Serializer() {
		super(Utf8.class);
	}

	public void serialize(Utf8 value, JsonGenerator jgen,
			SerializerProvider provider) throws IOException,
			JsonGenerationException {
		jgen.writeString(value.toString());
	}

	public final void serializeWithType(Utf8 value, JsonGenerator jgen,
			SerializerProvider provider, TypeSerializer typeSer)
			throws IOException, JsonGenerationException {
		serialize(value, jgen, provider);
	}

}
