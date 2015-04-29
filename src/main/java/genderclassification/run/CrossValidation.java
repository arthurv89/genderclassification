package genderclassification.run;

import static genderclassification.algorithm.naivebayesian.NaiveBayesianGenderModel.convertGenderToLetter;
import static genderclassification.utils.MathFunctions.round;
import genderclassification.algorithm.naivebayesian.NaiveBayesianModel;
import genderclassification.utils.DataParser;
import genderclassification.utils.DataTypes;

import java.io.IOException;
import java.util.Map;
import java.util.Random;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.FilterFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PObject;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.fn.FilterFns;

public class CrossValidation {
    private static final String MALE_FALSE = "Male False";
    private static final String MALE_TRUE = "Male True";
    private static final String FEMALE_TRUE = "Female True";
    private static final String FEMALE_FALSE = "Female False";
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
            final PCollection<String> testRowIds = userToGender.filter(testRowFilter).keys();
            // final PCollection<String> testRowIds = new MemCollection<>(Arrays.asList("2000581173"),
            // DataTypes.STRING_TYPE);

            final PTable<String, String> classifiedUsers = classificationAlgorithm.run(trainingRowIds, testRowIds);
            final long correctlyClassified = correctlyClassified(classifiedUsers);

            sumClassifiedUsers += testRowIds.length().getValue();
            sumCorrectlyClassified += correctlyClassified;

            printAllInformationOnResults(classifiedUsers, userToGender);
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

    private static void printConfusionMatrix(final PObject<Map<String, Long>> cm) {
        final long maleTrue = cm.getValue().get(MALE_TRUE) != null ? cm.getValue().get(MALE_TRUE) : 0;
        final long maleFalse = cm.getValue().get(MALE_FALSE) != null ? cm.getValue().get(MALE_FALSE) : 0;
        final long femaleFalse = cm.getValue().get(FEMALE_FALSE) != null ? cm.getValue().get(FEMALE_FALSE) : 0;
        final long femaleTrue = cm.getValue().get(FEMALE_TRUE) != null ? cm.getValue().get(FEMALE_TRUE) : 0;

        System.out.println();
        System.out.println("\tConfusion Matrix");
        System.out.println("\t\t\tPredicted Value");
        System.out.println("\t\t\tMale\t\tFemale");
        System.out.println("Real Value");
        System.out.println("Male\t\t\t" + maleTrue + "\t\t" + maleFalse);
        System.out.println("FeMale\t\t\t" + femaleFalse + "\t\t" + femaleTrue);

        Long trueSamples = femaleTrue + maleTrue;
        Long falseSamples = femaleFalse + maleFalse;
        Long allSamples = trueSamples + falseSamples;
        System.out.println("Accuracy: " + round((trueSamples / (double) allSamples) * 100, 2) + "%");
    };

    private void printAllInformationOnResults(final PTable<String, String> classifiedSamples,
            final PTable<String, String> userToGender) {

        final PTable<String, String> userToGenderReal = userToGender
                .join(classifiedSamples)
                .parallelDo(new DoFn<Pair<String, Pair<String, String>>, Pair<String, String>>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public void process(Pair<String, Pair<String, String>> input, Emitter<Pair<String, String>> emitter) {
                        emitter.emit(new Pair<String, String>(input.first(), input.second().first()));
                    }

                }, DataTypes.STRING_TO_STRING_TABLE_TYPE)
                .parallelDo(convertGenderToLetter, DataTypes.STRING_TO_STRING_TABLE_TYPE);

        final PTable<String, String> userToGenderClassified = classifiedSamples.parallelDo(convertGenderToLetter,
                DataTypes.STRING_TO_STRING_TABLE_TYPE);

        final PTable<String, Pair<String, String>> compare = userToGenderReal.join(userToGenderClassified);
        // TODO confusion matrix and put in cross validation

        final PTable<String, String> results = compare.parallelDo(
                new DoFn<Pair<String, Pair<String, String>>, Pair<String, String>>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public void process(Pair<String, Pair<String, String>> input, Emitter<Pair<String, String>> emitter) {
                        // TODO Auto-generated method stub
                        emitter.emit(new Pair<String, String>(compare(input.second().first(), input.second().second()),
                                "NONE"));
                    }

                    private String compare(String realClass, String classifiedClass) {
                        boolean correct = realClass.equalsIgnoreCase(classifiedClass);
                        if (correct) {
                            if (realClass.equals(NaiveBayesianModel.S_MALE))
                                return MALE_TRUE;
                            else
                                return FEMALE_TRUE;
                        } else {
                            if (realClass.equals(NaiveBayesianModel.S_MALE))
                                return MALE_FALSE;
                            else
                                return FEMALE_FALSE;
                        }
                    }

                }, DataTypes.STRING_TO_STRING_TABLE_TYPE);

        final PTable<String, Long> confusionMatrix = results.count().parallelDo(
                new DoFn<Pair<Pair<String, String>, Long>, Pair<String, Long>>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public void process(Pair<Pair<String, String>, Long> input, Emitter<Pair<String, Long>> emitter) {
                        emitter.emit(new Pair<String, Long>(input.first().first(), input.second()));
                    }
                }, DataTypes.STRING_TO_LONG_TYPE);

        printConfusionMatrix(confusionMatrix.asMap());
    }

}
