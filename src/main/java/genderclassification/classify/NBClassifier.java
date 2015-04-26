package genderclassification.classify;

import java.util.Collection;
import java.util.Map;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PObject;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.lib.join.DefaultJoinStrategy;
import org.apache.crunch.lib.join.JoinType;

import com.google.common.primitives.Doubles;

import genderclassification.algorithm.naivebayesian.NaiveBayesianModel;
import genderclassification.utils.DataParser;
import genderclassification.utils.DataTypes;
import genderclassification.utils.Mappers;
import static genderclassification.algorithm.naivebayesian.NaiveBayesianGenderModel.convertGenderToLetter;
import static genderclassification.utils.MathFunctions.round;

public class NBClassifier {
    private final NaiveBayesianModel model;

    public NBClassifier(final NaiveBayesianModel model) {
        this.model = model;
    }

    @SuppressWarnings("unchecked")
    public PTable<String, String> classifyNaiveBayes(final PCollection<String> userIds) {
        final PTable<String, String> userToProduct = DataParser.productUser();
        final PTable<String, String> productToCategory = DataParser.productCategory();

        final PTable<String, String> userToGender = userIds.parallelDo(new MapFn<String, Pair<String, String>>() {
            private static final long serialVersionUID = 8685387120655952971L;

            @Override
            public Pair<String, String> map(final String line) {
                final String[] values = line.split("\t");
                final String userId = values[0];
                final String genderProbabilities = values[1];
                return new Pair<String, String>(userId, genderProbabilities);
            }
        }, DataTypes.STRING_TO_STRING_TABLE_TYPE);

        final PTable<String, String> userToGenderString = userToGender.parallelDo(convertGenderToLetter,
                DataTypes.STRING_TO_STRING_TABLE_TYPE);

        final PTable<String, String> userToCategory = new DefaultJoinStrategy<String, String, String>()
                .join(userToProduct, productToCategory, JoinType.INNER_JOIN).values()
                .parallelDo(Mappers.IDENTITY, DataTypes.STRING_TO_STRING_TABLE_TYPE);

        final PTable<String, Collection<Double>> probUserIsM_Or_F = userToCategory.groupByKey().parallelDo(
                getProbMaleOrFemale, DataTypes.STRING_TO_DOUBLE_COLLECTION_TABLE_TYPE);

        @SuppressWarnings("rawtypes")
        final PTable<String, Pair<String, Collection<Double>>> probWithRealClass = new DefaultJoinStrategy().join(
                userToGenderString, probUserIsM_Or_F, JoinType.INNER_JOIN);

        final PTable<String, String> classifiedSamplesInNewScheme = probWithRealClass.parallelDo(classifyNBInNewScheme,
                DataTypes.STRING_TO_STRING_TABLE_TYPE);

        final PTable<String, String> classifiedSamples = probWithRealClass.parallelDo(classifyNB,
                DataTypes.STRING_TO_STRING_TABLE_TYPE);

        final PTable<String, Pair<String, String>> classifiedSamplesWithTrueClass = probWithRealClass.parallelDo(
                classifyNBOriginal, DataTypes.STRING_TO_PAIR_STRING_TABLE_TYPE);

        final PTable<String, Long> confusionMatrix = probWithRealClass
                .parallelDo(classifyNB_And_ComputeConfusionMatrix, DataTypes.STRING_TO_DOUBLE_TYPE).count()
                .parallelDo(new DoFn<Pair<Pair<String, Double>, Long>, Pair<String, Long>>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public void process(Pair<Pair<String, Double>, Long> input, Emitter<Pair<String, Long>> emitter) {
                        emitter.emit(new Pair<String, Long>(input.first().first(), input.second()));
                    }

                }, DataTypes.STRING_TO_LONG_TYPE);

        System.out.println("classified Sample With True Class:" + classifiedSamplesWithTrueClass);

        System.out.println("classified Samples With Decision: " + classifiedSamples);

        printConfusionMatrix(confusionMatrix.asMap());

        return classifiedSamplesInNewScheme;
    }

    private static void printConfusionMatrix(final PObject<Map<String, Long>> cm){
        System.out.println();
        System.out.println("\t\t\tPredicted Value");
        System.out.println("\t\t\tMale\t\tFemale");
        System.out.println("Real Value");
        System.out.println("Male\t\t\t"+(cm.getValue().get("Male True") != null ? cm.getValue().get("Male True") : 0)+"\t\t"+(cm.getValue().get("Male False") != null ? cm.getValue().get("Male False") : 0));
        System.out.println("FeMale\t\t\t"+(cm.getValue().get("Female True") != null ? cm.getValue().get("Female True") : 0)+"\t\t"+(cm.getValue().get("Female False") != null ? cm.getValue().get("Female False") : 0));        
    };
    
    private DoFn<Pair<String, Iterable<String>>, Pair<String, Collection<Double>>> getProbMaleOrFemale = new DoFn<Pair<String, Iterable<String>>, Pair<String, Collection<Double>>>() {

        private static final long serialVersionUID = 1L;

        @Override
        public void process(Pair<String, Iterable<String>> input, Emitter<Pair<String, Collection<Double>>> emitter) {
            double[] prob = new double[2];
            prob[NaiveBayesianModel.MALE] = 1;
            prob[NaiveBayesianModel.FEMALE] = 1;
            for (String category : input.second()) {
                prob[NaiveBayesianModel.MALE] *= model.getCategoryProb(category).get(NaiveBayesianModel.MALE);
                prob[NaiveBayesianModel.FEMALE] *= model.getCategoryProb(category).get(NaiveBayesianModel.FEMALE);
            }
            // multiply with prior
            prob[NaiveBayesianModel.MALE] *= model.getPrior(NaiveBayesianModel.S_MALE);
            prob[NaiveBayesianModel.FEMALE] *= model.getPrior(NaiveBayesianModel.S_FEMALE);
            // normalize
            double sum = prob[NaiveBayesianModel.MALE] + prob[NaiveBayesianModel.FEMALE];
            prob[NaiveBayesianModel.MALE] = round(prob[NaiveBayesianModel.MALE] / sum, 2);
            prob[NaiveBayesianModel.FEMALE] = round(prob[NaiveBayesianModel.FEMALE] / sum, 2);
            emitter.emit(new Pair<String, Collection<Double>>(input.first(), Doubles.asList(prob)));
        }

    };

    private DoFn<Pair<String, Pair<String, Collection<Double>>>, Pair<String, String>> classifyNB = new DoFn<Pair<String, Pair<String, Collection<Double>>>, Pair<String, String>>() {

        private static final long serialVersionUID = 1L;

        @Override
        public void process(Pair<String, Pair<String, Collection<Double>>> input, Emitter<Pair<String, String>> emitter) {
            emitter.emit(new Pair<String, String>(input.first(), compare(input.second().first(), convertToGender(input
                    .second().second()))));
        }

        private String convertToGender(Collection<Double> prob) {
            double[] p = Doubles.toArray(prob);
            return (p[NaiveBayesianModel.MALE] >= p[NaiveBayesianModel.FEMALE] ? NaiveBayesianModel.S_MALE
                    : NaiveBayesianModel.S_FEMALE);
        }

        private String compare(String realClass, String classifiedClass) {
            return (realClass.equalsIgnoreCase(classifiedClass) ? "True " + realClass : "False " + classifiedClass);
        }

    };

    private DoFn<Pair<String, Pair<String, Collection<Double>>>, Pair<String, Double>> classifyNB_And_ComputeConfusionMatrix = new DoFn<Pair<String, Pair<String, Collection<Double>>>, Pair<String, Double>>() {

        private static final long serialVersionUID = 1L;

        @Override
        public void process(Pair<String, Pair<String, Collection<Double>>> input, Emitter<Pair<String, Double>> emitter) {
            emitter.emit(new Pair<String, Double>(compare(input.second().first(), convertToGender(input.second()
                    .second())), 1.0));
        }

        private String convertToGender(Collection<Double> prob) {
            double[] p = Doubles.toArray(prob);
            return (p[NaiveBayesianModel.MALE] >= p[NaiveBayesianModel.FEMALE] ? NaiveBayesianModel.S_MALE
                    : NaiveBayesianModel.S_FEMALE);
        }

        private String compare(String realClass, String classifiedClass) {
            boolean correct = realClass.equalsIgnoreCase(classifiedClass);
            if (correct) {
                if (realClass.equals(NaiveBayesianModel.S_MALE))
                    return "Male True";
                else
                    return "Female True";
            } else {
                if (realClass.equals(NaiveBayesianModel.S_MALE))
                    return "Male False";
                else
                    return "Female False";
            }
        }

    };

    private DoFn<Pair<String, Pair<String, Collection<Double>>>, Pair<String, Pair<String, String>>> classifyNBOriginal = new DoFn<Pair<String, Pair<String, Collection<Double>>>, Pair<String, Pair<String, String>>>() {
        // <UserId, TrueClass, ClassifiedClass?
        private static final long serialVersionUID = 1L;

        @Override
        public void process(Pair<String, Pair<String, Collection<Double>>> input,
                Emitter<Pair<String, Pair<String, String>>> emitter) {
            emitter.emit(new Pair<String, Pair<String, String>>(input.first(), new Pair<String, String>(input.second()
                    .first(), convertToGender(input.second().second()))));
        }

        private String convertToGender(Collection<Double> prob) {
            double[] p = Doubles.toArray(prob);
            return (p[NaiveBayesianModel.MALE] >= p[NaiveBayesianModel.FEMALE] ? NaiveBayesianModel.S_MALE
                    : NaiveBayesianModel.S_FEMALE);
        }
    };

    private DoFn<Pair<String, Pair<String, Collection<Double>>>, Pair<String, String>> classifyNBInNewScheme = new DoFn<Pair<String, Pair<String, Collection<Double>>>, Pair<String, String>>() {

        private static final long serialVersionUID = 1L;

        @Override
        public void process(Pair<String, Pair<String, Collection<Double>>> input, Emitter<Pair<String, String>> emitter) {
            emitter.emit(new Pair<String, String>(input.first(), convertToGender(input.second().second())));
        }

        private String convertToGender(Collection<Double> prob) {
            double[] p = Doubles.toArray(prob);
            return (p[NaiveBayesianModel.MALE] >= p[NaiveBayesianModel.FEMALE] ? "1 0 0" : "0 1 0");
        }
    };
}
