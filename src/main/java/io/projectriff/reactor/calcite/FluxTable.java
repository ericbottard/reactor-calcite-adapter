package io.projectriff.reactor.calcite;

import java.util.Arrays;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.StreamableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import reactor.core.publisher.Flux;

import static org.apache.calcite.rel.RelFieldCollation.Direction.ASCENDING;

/**
 * A streaming SQL Table backed by a {@link Flux}.
 *
 * @author Eric Bottard
 * @param <T> the type of elements of the backing Flux
 */
public class FluxTable<T> extends AbstractTable implements ScannableTable, StreamableTable {
	private final Flux<T> flux;
	private final RowConverter<T> rowConverter;

	public FluxTable(Flux<T> flux, RowConverter<T> rowConverter) {
		this.flux = flux;
		this.rowConverter = rowConverter;
	}

	@Override
	public RelDataType getRowType(RelDataTypeFactory typeFactory) {
		return rowConverter.getRowType(typeFactory, "");
	}

	@Override
	public Enumerable<Object[]> scan(DataContext root) {
		return new AbstractEnumerable<>() {
			@Override
			public Enumerator<Object[]> enumerator() {
				return new IteratorEnumerator<>(flux.toIterable(), rowConverter);
			}
		};
	}

	@Override
	public Table stream() {
		return this;
	}

	@Override
	public Statistic getStatistic() {
		return Statistics.of(null,
				ImmutableList.of(),
				ImmutableList.of(),
				Arrays.asList(RelCollations.of(new RelFieldCollation(0 /*This is the index of "rowtime"*/, ASCENDING)))
		);
	}


}
