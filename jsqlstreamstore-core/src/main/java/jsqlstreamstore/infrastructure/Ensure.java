package jsqlstreamstore.infrastructure;

import java.util.Optional;

public final class Ensure {

	private Ensure() {
		// static utility
	}

	public static boolean isNullOrEmpty(String string) {
		return stringIsNullOrEmpty(string);
	}

	public static boolean stringIsNullOrEmpty(String string) {
		return string == null || string.isEmpty();
	}

	public static <T> T notNull(T t) {
		if (t == null) {
			throw new NullPointerException();
		}
		return t;
	}

	public static void notNullOrEmpty(String argument, String argumentName) {
		if (isNullOrEmpty(argument)) {
			throw new IllegalArgumentException(argument);
		}
	}

	public static void positive(int number, String argumentName) {
		if (number <= 0) {
			throw new IllegalArgumentException(argumentName + " should be positive.");
		}
	}

	public static void positive(Optional<Long> number, String argumentName) {
	    if (number.isPresent() && number.get() <= 0) {
	        throw new IllegalArgumentException(argumentName + " should be positive.");
	    }
	}
	
	public static void positive(long number, String argumentName) {
		if (number <= 0) {
			throw new IllegalArgumentException(argumentName + " should be positive.");
		}
	}

	public static void nonnegative(long number, String argumentName) {
		if (number < 0) {
			throw new IllegalArgumentException(argumentName + " should be non-negative.");
		}
	}

	public static void nonnegative(int number, String argumentName) {
		if (number < 0) {
			throw new IllegalArgumentException(argumentName + " should be non-negative.");
		}
	}

	public static void equal(int expected, int actual) {
		if (expected != actual) {
		    // TODO: should we use java.text.MessageFormat or concat?
			throw new IllegalArgumentException(String.format("expected %s actual %s", expected, actual));
		}
	}
}
