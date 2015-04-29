package genderclassification.algorithm.naivebayesian;

import java.util.Collection;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.lib.join.DefaultJoinStrategy;
import org.apache.crunch.lib.join.JoinType;

import com.google.common.primitives.Doubles;

import genderclassification.utils.DataParser;
import genderclassification.utils.DataTypes;
import genderclassification.utils.Mappers;
import static genderclassification.utils.MathFunctions.round;

public class NBClassifier {
    private final NaiveBayesianModel model;
    private static boolean switchNormalized = true;

    public NBClassifier(final NaiveBayesianModel model) {
        this.model = model;
    }

    public static void setSwitchNormalized(boolean on){
        switchNormalized = on;
    }
    
    public PTable<String, String> classifyNaiveBayes(final PCollection<String> userIds) {
        final PTable<String, String> userToProduct = DataParser.productUser();
        final PTable<String, String> productToCategory = DataParser.productCategory();

        final PTable<String, String> userToGender = userIds.parallelDo(new MapFn<String, Pair<String, String>>() {
            private static final long serialVersionUID = 8685387120655952971L;

            @Override
            public Pair<String, String> map(final String line) {
                return new Pair<String, String>(line, "NONE");
            }
        }, DataTypes.STRING_TO_STRING_TABLE_TYPE);

        final PTable<String, String> userToCategory = new DefaultJoinStrategy<String, String, String>()
                .join(userToProduct, productToCategory, JoinType.INNER_JOIN).values()
                .parallelDo(Mappers.IDENTITY, DataTypes.STRING_TO_STRING_TABLE_TYPE);

        final PTable<String, String> userToCategoryFiltered = new DefaultJoinStrategy<String, String, String>().join(
                userToGender, userToCategory, JoinType.INNER_JOIN).parallelDo(
                new DoFn<Pair<String, Pair<String, String>>, Pair<String, String>>() {

                    private static final long serialVersionUID = 11212313123124L;

                    @Override
                    public void process(Pair<String, Pair<String, String>> input, Emitter<Pair<String, String>> emitter) {
                        emitter.emit(new Pair<String, String>(input.first(), input.second().second()));
                    }

                }, DataTypes.STRING_TO_STRING_TABLE_TYPE);

        System.out.println(userToCategoryFiltered);

        final PTable<String, Collection<Double>> probUserIsM_Or_F = userToCategoryFiltered.groupByKey().parallelDo(
                getProbMaleOrFemale, DataTypes.STRING_TO_DOUBLE_COLLECTION_TABLE_TYPE);

        final PTable<String, String> classifiedSamplesInNewScheme = probUserIsM_Or_F.parallelDo(classifyNBInNewScheme,
                DataTypes.STRING_TO_STRING_TABLE_TYPE);

        return classifiedSamplesInNewScheme;
    }

    
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
            if(switchNormalized){
                double sum = prob[NaiveBayesianModel.MALE] + prob[NaiveBayesianModel.FEMALE];
                prob[NaiveBayesianModel.MALE] = round(prob[NaiveBayesianModel.MALE] / sum, 2);
                prob[NaiveBayesianModel.FEMALE] = round(prob[NaiveBayesianModel.FEMALE] / sum, 2);
            }
            emitter.emit(new Pair<String, Collection<Double>>(input.first(), Doubles.asList(prob)));
        }

    };

    private DoFn<Pair<String, Collection<Double>>, Pair<String, String>> classifyNBInNewScheme = new DoFn<Pair<String, Collection<Double>>, Pair<String, String>>() {

        private static final long serialVersionUID = 1L;

        @Override
        public void process(Pair<String, Collection<Double>> input, Emitter<Pair<String, String>> emitter) {
            emitter.emit(new Pair<String, String>(input.first(), convertToGender(input.second())));
        }

        private String convertToGender(Collection<Double> prob) {
            double[] p = Doubles.toArray(prob);
            return (p[NaiveBayesianModel.MALE] >= p[NaiveBayesianModel.FEMALE] ? "1 0 0" : "0 1 0");
        }
    };
}
