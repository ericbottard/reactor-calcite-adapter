package io.projectriff.reactor.calcite;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * An example implementation of RowConverter, where the source data is a {@code Flux<Long>}.
 * <p>Adds a virtual {@code rowtime} column which is ascending (see {@link FluxTable#getStatistic()} and represents the
 * timestamp at which data is seen.</p>
 *
 * <p>An implementation dealing with CloudEvents or riff {@code InputFrames} would expose each attribute as a column, and
 * maybe even introspect the {@type data} with some extra info (<em>e.g.</em> flatten JSON).</p>
 *
 * <p>Ultimately, it is this strategy object which should expose statistics about columns, according to user knowledge of the data.</p>
 *
 * @author Eric Bottard
 */
public class ExampleRowConverter implements RowConverter<Long> {

	@Override
	public RelDataType getRowType(RelDataTypeFactory typeFactory, String tableName) {
		final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
		fieldInfo.add("ROWTIME", typeFactory.createSqlType(SqlTypeName.TIMESTAMP));
		fieldInfo.add("A", typeFactory.createSqlType(SqlTypeName.INTEGER));
		return fieldInfo.build();
	}

	@Override
	public Object[] toRow(Long raw) {
		return new Object[] {System.currentTimeMillis(), raw};
	}
}
