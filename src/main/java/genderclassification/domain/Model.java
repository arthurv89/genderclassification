package genderclassification.domain;

import java.util.List;

import com.google.common.collect.ImmutableMap;

public class Model {
	private final ImmutableMap<String, List<Double>> genderProbabilityMap;
	
	public Model(final ImmutableMap<String, List<Double>> genderProbabilityMap) {
		this.genderProbabilityMap = genderProbabilityMap;
	}

	public List<Double> get(final String gender) {
		return genderProbabilityMap.get(gender);
	}
}
