package genderclassification.createmodel;

import genderclassification.domain.Category;
import genderclassification.utils.DataParser;
import genderclassification.utils.DataTypes;
import genderclassification.utils.Mappers;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

import org.apache.crunch.CombineFn;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.lib.join.DefaultJoinStrategy;
import org.apache.crunch.lib.join.JoinType;

import com.google.common.primitives.Doubles;

public class GenderModel implements Serializable {
    public static PTable<String, Collection<Double>> determineModel(final PCollection<String> userProductLines,
            final PCollection<String> userGenderLines, final PCollection<String> classifiedUserLines,
            final PCollection<String> productCategoryLines) {
        // Parse the data files
        final PTable<String, String> productToUser = DataParser.productUser(userProductLines);
        final PTable<String, String> userToGender = DataParser.userGender(userGenderLines);
        final PTable<String, String> productToCategory = DataParser.productCategory(productCategoryLines);
        final PTable<String, String> classifiedUserToGender = DataParser.classifiedUserGender(classifiedUserLines);

        final PTable<String, String> userToCategory = new DefaultJoinStrategy<String, String, String>()
        // (P,U)* JOIN (P,C) = (P, (U,C))*
                .join(productToUser, productToCategory, JoinType.INNER_JOIN)
                // (U,C)
                .values()
                // (U,C)
                .parallelDo(Mappers.IDENTITY, DataTypes.STRING_TO_STRING_TABLE_TYPE);

        // print(productToUser, "productToUser");
        // print(productToCategory, "productToCategory");
        // System.out.println(userToCategory);

        final PTable<String, String> allUsersToGender = userToGender.union(classifiedUserToGender);

        final PTable<String, Pair<String, String>> join = new DefaultJoinStrategy<String, String, String>()
        // (U,G) JOIN (U,C) = (U,(G,C))
                .join(allUsersToGender, userToCategory, JoinType.INNER_JOIN);

        // print(userToGender, "userToGender");
        // print(userToCategory, "userToCategory");
        // System.out.println(join);

        return join
        // (G,C)
                .values()
                // (GC,prob)*
                .parallelDo(toGenderAndCategoryPair_probability, DataTypes.PAIR_STRING_STRING_TO_DOUBLE_TABLE_TYPE)
                // (GC,[prob])
                .groupByKey()
                // (GC,prob)
                .combineValues(Aggregators.SUM_DOUBLES())
                // (GC,freq)*
                .parallelDo(toGender_frequencies, DataTypes.STRING_TO_DOUBLE_COLLECTION_TABLE_TYPE)
                // (G,[freq])
                .groupByKey()
                // (G, freqVector)
                .combineValues(sumFrequencies);
    }

    private static final long serialVersionUID = -6683089099880990848L;

    private static final int CATEGORY_COUNT = Category.countCategories();

    private static final DoFn<Pair<String, String>, Pair<Pair<String, String>, Double>> toGenderAndCategoryPair_probability = new DoFn<Pair<String, String>, Pair<Pair<String, String>, Double>>() {
        private static final long serialVersionUID = 124672868421678412L;

        @Override
        public void process(final Pair<String, String> input, final Emitter<Pair<Pair<String, String>, Double>> emitter) {
            final String genderProbabilities = input.first();
            final String category = input.second();

            final String[] probabilityPerGender = genderProbabilities.split(" ");

            emitter.emit(pair(Gender.M, category, probabilityPerGender));
            emitter.emit(pair(Gender.F, category, probabilityPerGender));
            emitter.emit(pair(Gender.U, category, probabilityPerGender));
        }

        private Pair<Pair<String, String>, Double> pair(final Gender gender, final String category,
                final String[] probabilityPerGender) {
            final Pair<String, String> genderAndCategory = new Pair<>(gender.name(), category);
            final Double probability = Double.parseDouble(probabilityPerGender[gender.position]);
            return new Pair<Pair<String, String>, Double>(genderAndCategory, probability);
        }
    };

    private static final DoFn<Pair<Pair<String, String>, Double>, Pair<String, Collection<Double>>> toGender_frequencies = new DoFn<Pair<Pair<String, String>, Double>, Pair<String, Collection<Double>>>() {
        private static final long serialVersionUID = 1121312312323123423L;

        @Override
        public void process(final Pair<Pair<String, String>, Double> input,
                final Emitter<Pair<String, Collection<Double>>> emitter) {
            final String gender = input.first().first();
            final String category = input.first().second();
            final Double frequency = input.second();

            final List<Double> frequencies = createCollection(new Pair<>(Category.getIndex(category), frequency));
            emitter.emit(new Pair<String, Collection<Double>>(gender, frequencies));
        }

        private List<Double> createCollection(final Pair<Integer, Double> categoryIndex_FrequencyPair) {
            final Integer categoryIndex = categoryIndex_FrequencyPair.first();
            final Double probability = categoryIndex_FrequencyPair.second();

            final double frequency[] = new double[CATEGORY_COUNT];
            frequency[categoryIndex] = probability;
            return Doubles.asList(frequency);
        }
    };

    private static CombineFn<String, Collection<Double>> sumFrequencies = new CombineFn<String, Collection<Double>>() {
        private static final long serialVersionUID = 8834826647974920164L;

        @Override
        public void process(final Pair<String, Iterable<Collection<Double>>> input,
                final Emitter<Pair<String, Collection<Double>>> emitter) {
            final String gender = input.first();
            final Iterable<Collection<Double>> frequenciesIterable = input.second();

            final double[] sum = new double[CATEGORY_COUNT];
            for (final Collection<Double> frequencies : frequenciesIterable) {
                final List<Double> frequencyList = (List<Double>) frequencies;
                for (int idx = 0; idx < frequencyList.size(); idx++) {
                    sum[idx] += frequencyList.get(idx);
                }
            }

            emitter.emit(new Pair<String, Collection<Double>>(gender, Doubles.asList(sum)));
        }
    };

    private enum Gender {
        M(0), F(1), U(2);

        private final int position;

        private Gender(final int position) {
            this.position = position;
        }
    }
}
