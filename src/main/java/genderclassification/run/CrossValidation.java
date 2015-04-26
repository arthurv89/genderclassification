package genderclassification.run;

import genderclassification.algorithm.naivebayesian.NaiveBayesianClassification;
import genderclassification.utils.DataParser;
import genderclassification.utils.DataTypes;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.apache.crunch.FilterFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.fn.FilterFns;
import org.apache.crunch.impl.mem.collect.MemCollection;

public class CrossValidation {
    private static final int TRAIN_TEST_RATIO = 3;
    private static final int ITERATIONS = 1;
    private static final PTable<String, String> userToGender = DataParser.userGender();
    private final Random random;

    public CrossValidation(final int seed) {
        random = new Random(seed);
    }

    public double performCrossValidation(final ClassificationAlgorithm classificationAlgorithm) throws IOException {
        classificationAlgorithm.initialize();

        long sumCorrectlyClassified = 0;
        long sumClassifiedUsers = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            final FilterFn<Pair<String, String>> testRowFilter = testRowFilter(random.nextLong());
            final PTable<String, String> trainingRowIds = userToGender.filter(FilterFns.not(testRowFilter));
            // final PCollection<String> testRowIds = userToGender.filter(testRowFilter).keys();
            PCollection<String> testRowIds = new MemCollection<>(Arrays.asList("2000581173"), DataTypes.STRING_TYPE);

            if (classificationAlgorithm instanceof NaiveBayesianClassification) {
                testRowIds = new MemCollection<>(Arrays.asList("2000581173\t1 0 0"), DataTypes.STRING_TYPE);
            }

            final PTable<String, String> classifiedUsers = classificationAlgorithm.run(trainingRowIds, testRowIds);
            final long correctlyClassified = correctlyClassified(classifiedUsers);

            sumClassifiedUsers += testRowIds.length().getValue();
            sumCorrectlyClassified += correctlyClassified;
        }
        return sumCorrectlyClassified / (double) sumClassifiedUsers;
    }

    private Long correctlyClassified(final PTable<String, String> classifiedUsers) {
        return classifiedUsers.join(userToGender).filter(classificationCorrect).length().getValue();
    }

    private static FilterFn<Pair<String, Pair<String, String>>> classificationCorrect = new FilterFn<Pair<String, Pair<String, String>>>() {
        private static final long serialVersionUID = 1L;

        public boolean accept(final Pair<String, Pair<String, String>> pair) {
            boolean correctClassification = pair.second().first().equals(pair.second().second());
            return correctClassification;
        }

    };

    private static final FilterFn<Pair<String, String>> testRowFilter(final long randomLong) {
        return new FilterFn<Pair<String, String>>() {
            private static final long serialVersionUID = 531980539810L;

            public boolean accept(final Pair<String, String> input) {
                long userId = Long.parseLong(input.first());
                // long xorId = randomLong ^ userId;
                // return (xorId % TRAIN_TEST_RATIO) == 0;
                return userId % TRAIN_TEST_RATIO == 0;
            }
        };
    };
}
