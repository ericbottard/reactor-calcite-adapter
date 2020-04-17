package io.projectriff.reactor.calcite;

import java.util.Map;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AggregateFunctionImpl;

public class ReactorSchema extends AbstractSchema {
	private Map<String, Table> tableMap;

	public ReactorSchema(Map<String, Table> tableMap) {
		this.tableMap = tableMap;
	}

	@Override
	protected Map<String, Table> getTableMap() {
		return tableMap;
	}

	@Override
	protected Multimap<String, Function> getFunctionMultimap() {
		return ImmutableMultimap.of("SOMEFUN", AggregateFunctionImpl.create(MyAggregateFunction.class));
	}
}

