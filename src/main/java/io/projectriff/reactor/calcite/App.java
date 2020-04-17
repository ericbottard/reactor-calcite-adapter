package io.projectriff.reactor.calcite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) throws Exception {

		Flux<Long> flux = Flux.interval(Duration.ofMillis(250L)).take(20);

		String sql = "SELECT STREAM rowtime, A FROM foobar WHERE mod(A, 2) = 0";
		//String sql = "SELECT STREAM CEIL(rowtime TO SECOND), SOMEFUN(A), count(*) FROM flux GROUP BY CEIL(rowtime TO SECOND) ORDER BY CEIL(rowtime TO SECOND)";

		flux.transform(SQLQuery.query(sql)).subscribe(a -> {System.out.println(Arrays.asList((Object[])a));});

	}
}
