package io.projectriff.reactor.calcite;

/**
 * Example of an aggregate function, that accumulates states (using static methods or not).
 *
 * Maybe this could be used to invoke the rpc function, in constructs like
 * <pre>
 *     SELECT myfunction(cols), CEIL(rowtime to MINUTE) FROM flux GROUP BY CEIL(rowtime to minute)
 * </pre>? The SELECTed columns doesn't have to produce anything useful, but needs to encompass all the relevant columns
 * to pass to the function.
 *
 * @author Eric Bottard
 */
public class MyAggregateFunction {

	// A init()
	// A add(A, V)
	// A merge(A, A)
	// R result(A)

	private Integer state = 0;

	public int init() {
		System.out.println("init");
		return 0;
	}

	public int add(int a, int v) {
		System.out.format("add(%d, %d)%n", a, v);
		return state = a + v;
	}

	public int merge(int a, int b) {
		state = a + b;
		return state;
	}

	public int result(int a) {
		System.out.printf("result(%d)%n", a);
		return state = a;
	}
}
