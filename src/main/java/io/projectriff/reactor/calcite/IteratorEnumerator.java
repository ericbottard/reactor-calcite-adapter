package io.projectriff.reactor.calcite;

import java.util.Iterator;

import org.apache.calcite.linq4j.Enumerator;

/**
 * Adapter between an {@link Iterator} and Calcite's {@link Enumerator} of column values.
 *
 * <p>Conversion from type {@code T} to column values is handled by a {@link RowConverter}</p>
 *
 * @author Eric Bottard
 * @param <T> The original type iterated over
 */
public class IteratorEnumerator<T> implements Enumerator<Object[]> {

	private Object[] current;
	private Iterator<T> iterator;
	private final RowConverter<T> rowConverter;

	public IteratorEnumerator(Iterable<T> source, RowConverter<T> rowConverter) {
		iterator = source.iterator();
		this.rowConverter = rowConverter;
	}

	@Override
	public Object[] current() {
		return current;
	}

	@Override
	public boolean moveNext() {
		boolean b = iterator.hasNext();
		if (b) {
			current = rowConverter.toRow(iterator.next());
		}
		return b;
	}

	@Override
	public void reset() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void close() {
	}
}
