package genderclassification.classify;

import java.util.Collection;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.lib.join.DefaultJoinStrategy;
import org.apache.crunch.lib.join.JoinType;

import com.google.common.primitives.Doubles;

import genderclassification.domain.NBModel;
import genderclassification.utils.DataParser;
import genderclassification.utils.DataTypes;
import genderclassification.utils.Mappers;
import static genderclassification.createmodel.GenderModel.convertGenderToLetter;
import static genderclassification.utils.MathFunctions.round;

public class NBClassifier {
    private final NBModel model;

    public NBClassifier(final NBModel model) {
        this.model = model;
    }

    @SuppressWarnings("unchecked")
    public PTable<String, Long> classifyNaiveBayes(final PCollection<String> userProductLines,
            final PCollection<String> userGenderLines, final PCollection<String> productCategoryLines) {
        final PTable<String, String> userToProduct = DataParser.productUser(userProductLines);
        final PTable<String, String> userToGender = DataParser.userGender(userGenderLines);
        final PTable<String, String> productToCategory = DataParser.productCategory(productCategoryLines);

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

        final PTable<String, String> classifiedSamples = probWithRealClass.parallelDo(classifyNB,
                DataTypes.STRING_TO_STRING_TABLE_TYPE);

        final PTable<String, Long> confusionMatrix = probWithRealClass
                .parallelDo(classifyNB_And_ComputeConfusionMatrix, DataTypes.STRING_TO_DOUBLE_TYPE).count()
                .parallelDo(new DoFn<Pair<Pair<String, Double>, Long>, Pair<String, Long>>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public void process(Pair<Pair<String, Double>, Long> input, Emitter<Pair<String, Long>> emitter) {
                        emitter.emit(new Pair<String, Long>(input.first().first(), input.second()));
                    }

                }, DataTypes.STRING_TO_LONG_TYPE);

        System.out.println(classifiedSamples);

        System.out.println(confusionMatrix);

        return confusionMatrix;
    }

    private DoFn<Pair<String, Iterable<String>>, Pair<String, Collection<Double>>> getProbMaleOrFemale = new DoFn<Pair<String, Iterable<String>>, Pair<String, Collection<Double>>>() {

        private static final long serialVersionUID = 1L;

        @Override
        public void process(Pair<String, Iterable<String>> input, Emitter<Pair<String, Collection<Double>>> emitter) {
            double[] prob = new double[2];
            prob[NBModel.MALE] = 1;
            prob[NBModel.FEMALE] = 1;
            for (String category : input.second()) {
                prob[NBModel.MALE] *= model.getCategoryProb(category).get(NBModel.MALE);
                prob[NBModel.FEMALE] *= model.getCategoryProb(category).get(NBModel.FEMALE);
            }
            // multiply with prior
            prob[NBModel.MALE] *= model.getPrior(NBModel.S_MALE);
            prob[NBModel.FEMALE] *= model.getPrior(NBModel.S_FEMALE);
            // normalize
            double sum = prob[NBModel.MALE] + prob[NBModel.FEMALE];
            prob[NBModel.MALE] = round(prob[NBModel.MALE] / sum, 2);
            prob[NBModel.FEMALE] = round(prob[NBModel.FEMALE] / sum, 2);
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
            return (p[NBModel.MALE] >= p[NBModel.FEMALE] ? NBModel.S_MALE : NBModel.S_FEMALE);
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
            return (p[NBModel.MALE] >= p[NBModel.FEMALE] ? NBModel.S_MALE : NBModel.S_FEMALE);
        }

        private String compare(String realClass, String classifiedClass) {
            boolean correct = realClass.equalsIgnoreCase(classifiedClass);
            if (correct) {
                if (realClass.equals(NBModel.S_MALE))
                    return "Male True";
                else
                    return "Female True";
            } else {
                if (realClass.equals(NBModel.S_MALE))
                    return "Male False";
                else
                    return "Female False";
            }
        }

    };

}
