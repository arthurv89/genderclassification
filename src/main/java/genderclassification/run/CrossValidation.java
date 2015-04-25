package genderclassification.run;

import genderclassification.utils.DataParser;

import java.io.IOException;
import java.util.Random;

import org.apache.crunch.FilterFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.fn.FilterFns;

public class CrossValidation {
	private static final double TRAIN_TEST_RATIO = 3.0;
	private static final int ITERATIONS = 10;
	private static final PTable<String, String> userToGender = DataParser.userGender();
	private static final long userToGenderSize = userToGender.length().getValue();
	private final Random random;
	
	public CrossValidation(final int seed) {
		random = new Random(seed);
	}

	public double performCrossValidation(final ClassificationAlgorithm classificationAlgorithm) throws IOException {
		long sumCorrectlyClassified = 0;
		for(int i=0; i<ITERATIONS; i++) {
			final FilterFn<Pair<String, String>> testRowFilter = testRowFilter(random.nextLong());
			final PTable<String, String> trainingRowIds = userToGender.filter(FilterFns.not(testRowFilter));
			final PCollection<String> testRowIds = userToGender.filter(testRowFilter).keys();
			final PTable<String, String> classifiedUsers = classificationAlgorithm.run(trainingRowIds, testRowIds);
			
			final long correctlyClassified = correctlyClassified(classifiedUsers);
			sumCorrectlyClassified += correctlyClassified;
		}
		return sumCorrectlyClassified / (double) ITERATIONS / (double) userToGenderSize;
	}

	private Long correctlyClassified(final PTable<String, String> classifiedUsers) {
		return classifiedUsers.join(userToGender)
			.values()
			.filter(classificationCorrect)
			.length()
			.getValue();
	}

	private static FilterFn<Pair<String, String>> classificationCorrect = new FilterFn<Pair<String, String>>() {
		private static final long serialVersionUID = 1L;

		public boolean accept(final Pair<String, String> pair) {
			return pair.first().equals(pair.second());
		}
		
	};
	
	private static final FilterFn<Pair<String, String>> testRowFilter(final long randomLong) {
		return new FilterFn<Pair<String, String>>() {
			private static final long serialVersionUID = 531980539810L;
	
			public boolean accept(final Pair<String, String> input) {
				return (randomLong & Long.parseLong(input.first())) % TRAIN_TEST_RATIO == 0;
			}
		};
	};
}
