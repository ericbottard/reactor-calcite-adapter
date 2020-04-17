package io.projectriff.reactor.calcite;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

/**
 * Interface used to "columnize" a instance of {@code T} to SQL types.
 *
 * <p>Currently instanciated for the whole schema, but could easily be made per-table.</p>
 *
 * @author Eric Bottard
 * @param <T> the type seen by the source Flux
 */
public interface RowConverter<T> {

	/**
	 * Return the types of columns for the given "table".
	 */
	RelDataType getRowType(RelDataTypeFactory typeFactory, String tableName);

	/**
	 * Convert an instance of T to a set of column values.
	 */
	Object[] toRow(T raw);
}
