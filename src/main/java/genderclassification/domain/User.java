package genderclassification.domain;

import java.util.Arrays;
import java.util.List;

public class User {
	private final List<String> productIds;

	public User(final List<String> productIds) {
		this.productIds = productIds;
	}

	public User(final String... productIds) {
		this(Arrays.asList(productIds));
	}

	public List<String> getProductIds() {
		return productIds;
	}
	
	@Override
	public String toString() {
		return "User:"+ productIds;
	}
}
