package io.projectriff.reactor.calcite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

/**
 * Provides SQL functionality over Fluxes.
 *
 * @author Eric Bottard
 */
public class SQLQuery {

	/**
	 * Return a transformation function from a source {@code Flux<I>} to a {@code Flux<O>}, applying an SQL query using Calcite.
	 *
	 * <p>Could apply a transformation back from {@code Object[]} to some type {@code O}, or that could be implemented
	 * as a UDF in the schema.</p>
	 */
	public static <I, O> Function<Flux<I>, Publisher<O>> query(String sql) {
		return flux -> {
			ReactorSchemaFactory.instance = flux;
			try {
				Class.forName("org.apache.calcite.jdbc.Driver");
				// TODO: Apparently usage of a schema description file is optional and values can be passed inline
				Connection connection = DriverManager.getConnection("jdbc:calcite:model=reactor-model.json");
				Statement statement = connection.createStatement();
				ResultSet resultSet = statement.executeQuery(sql);
				ResultSetMetaData rsmd = resultSet.getMetaData();

				Flux<Object[]> resultFlux = Flux.generate(ss -> {
					try {
						if (resultSet.next()) {
							Object[] item = new Object[rsmd.getColumnCount()];
							for (int i = 1; i <= rsmd.getColumnCount(); i++) {
								item[i - 1] = resultSet.getString(i);
							}
							ss.next(item);
						}
						else {
							ss.complete();
						}
					}
					catch (SQLException e) {
						ss.error(e);
					}
				});
				return (Publisher<O>) resultFlux;
			}
			catch (SQLException | ClassNotFoundException e) {
				return Flux.error(e);
			}

		};
	}
}
