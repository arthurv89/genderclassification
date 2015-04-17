package genderclassification.model;

import org.apache.avro.reflect.Stringable;

@Stringable
public class GenderProbabilities {
	private final double maleProbability;
	private final double femaleProbability;
	private final double unknownProbability;
	
	public GenderProbabilities(String[] probabilities) {
		maleProbability = Double.parseDouble(probabilities[0]);
		femaleProbability = Double.parseDouble(probabilities[1]);
		unknownProbability = Double.parseDouble(probabilities[2]);
	}
}
