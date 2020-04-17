package io.projectriff.reactor.calcite;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import reactor.core.publisher.Flux;

public class ReactorSchemaFactory implements SchemaFactory {

	/**
	 * This is pretty ugly and non-scalable.
	 * <p>One workaround could be for users of this factory to specify the {@code ClassName#instance} coordinates
	 * of the field they want exposed as source.</p>
	 */
	public static Flux<?> instance;


	public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {

		Map<String, Table> tableMap = new HashMap<>();

		String rowConverterClass = (String) operand.get("rowConverter");
		try {
			Class<? extends RowConverter<?>> clazz = (Class<? extends RowConverter<?>>) Class.forName(rowConverterClass);
			RowConverter<?> rowConverter = clazz.getDeclaredConstructor().newInstance();
			tableMap.put("FOOBAR", new FluxTable(instance, rowConverter));
			return new ReactorSchema(tableMap);
		}
		catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
			throw new RuntimeException(e);
		}
	}
}
