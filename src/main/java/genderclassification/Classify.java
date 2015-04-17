package genderclassification;

import genderclassification.domain.Category;
import genderclassification.model.GenderProbabilities;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.crunch.CombineFn;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.fn.IdentityFn;
import org.apache.crunch.lib.join.DefaultJoinStrategy;
import org.apache.crunch.lib.join.JoinType;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.Avros;

public class Classify implements Serializable {

    private static final long serialVersionUID = -6683089099880990848L;

    private static final IdentityFn<Pair<String, String>> IDENTITY = IdentityFn.<Pair<String, String>> getInstance();
    private static final AvroType<String> STRING_TYPE = Avros.strings();
    private static final AvroType<GenderProbabilities> GENDER_PROBABILITY_TYPE = Avros
            .records(GenderProbabilities.class);
    private static final AvroType<Collection<Double>> ARRAY_DOUBLE = Avros.collections(Avros.doubles());

    private static final PTableType<String, String> STRING_TABLE = Avros.keyValueTableOf(STRING_TYPE, STRING_TYPE);
    private static final PTableType<String, GenderProbabilities> GENDER_PROBABILITY_TABLE = Avros.keyValueTableOf(
            STRING_TYPE, GENDER_PROBABILITY_TYPE);

    private static final AvroType<Pair<String, String>> PAIR_STRING_STRING_TYPE = Avros.pairs(STRING_TYPE, STRING_TYPE);
    private static final AvroType<Double> DOUBLE_TYPE = Avros.doubles();
    private static final AvroType<Integer> INTEGER_TYPE = Avros.ints();
    private static final AvroType<Pair<Integer, Double>> PAIR_INTEGER_DOUBLE_TYPE = Avros.pairs(INTEGER_TYPE,
            DOUBLE_TYPE);

    private static PTableType<Pair<String, String>, Double> categoryGenderToProbabilityType = Avros.keyValueTableOf(
            PAIR_STRING_STRING_TYPE, DOUBLE_TYPE);

    private static PTableType<String, Pair<Integer, Double>> genderToCategoryProbabilityType = Avros.keyValueTableOf(
            STRING_TYPE, PAIR_INTEGER_DOUBLE_TYPE);

    private static PTableType<String, Collection<Double>> tableOfStringToDoubles = Avros.keyValueTableOf(STRING_TYPE,
            ARRAY_DOUBLE);

    private final static MapFn<String, Pair<String, String>> parseUserProductLine = new MapFn<String, Pair<String, String>>() {
        private static final long serialVersionUID = 4431093387533962416L;

        @Override
        public Pair<String, String> map(final String line) {
            final String[] values = line.split("\t");
            final String userId = values[0];
            final String productId = values[1];
            return new Pair<String, String>(productId, userId);
        }
    };

    private final static MapFn<String, Pair<String, String>> parseProductCategoryLine = new MapFn<String, Pair<String, String>>() {
        private static final long serialVersionUID = 8685387020655952971L;

        @Override
        public Pair<String, String> map(final String line) {
            final String[] values = line.split("\t");
            final String productId = values[0];
            final String category = values[1];
            return new Pair<String, String>(productId, category);
        }
    };

    private final static DoFn<String, Pair<String, GenderProbabilities>> parseUserGenderLine = new MapFn<String, Pair<String, GenderProbabilities>>() {
        private static final long serialVersionUID = 8685387120655952971L;

        @Override
        public Pair<String, GenderProbabilities> map(final String line) {
            final String[] values = line.split("\t");
            final String userId = values[0];
            final String[] probabilities = values[1].split(" ");
            final GenderProbabilities genderProbabilities = new GenderProbabilities(probabilities);
            return new Pair<String, GenderProbabilities>(userId, genderProbabilities);
        }
    };

    public static PTable<String, Collection<Double>> classifyUsers(final PCollection<String> userProductLines,
            final PCollection<String> userGenderLines, final PCollection<String> productCategoryLines) {
        final PTable<String, String> productToUser = userProductLines.parallelDo(parseUserProductLine, STRING_TABLE);
        final PTable<String, String> productToCategory = productCategoryLines.parallelDo(parseProductCategoryLine,
                STRING_TABLE);
        final PTable<String, GenderProbabilities> userToGender = userGenderLines.parallelDo(parseUserGenderLine,
                GENDER_PROBABILITY_TABLE);

        final PTable<String, String> userToCategories = new DefaultJoinStrategy<String, String, String>()
                .join(productToUser, productToCategory, JoinType.INNER_JOIN).values()
                .parallelDo(IDENTITY, STRING_TABLE);

        // userToCategories.materialize().forEach(p -> {
        // System.out.println(p.first() + " " + p.second());
        // });

        final PCollection<Pair<String, GenderProbabilities>> userGenderToCategories = new DefaultJoinStrategy<String, String, GenderProbabilities>()
                .join(userToCategories, userToGender, JoinType.INNER_JOIN).values();

        final PTable<Pair<String, String>, Double> x = userGenderToCategories.parallelDo(
                new DoFn<Pair<String, GenderProbabilities>, Pair<Pair<String, String>, Double>>() {
                    private static final long serialVersionUID = 124672868421678412L;

                    @Override
                    public void process(final Pair<String, GenderProbabilities> input,
                            final Emitter<Pair<Pair<String, String>, Double>> emitter) {
                        emitter.emit(pair("M", input.second().getMaleProbability(), input.first()));
                        emitter.emit(pair("F", input.second().getFemaleProbability(), input.first()));
                        emitter.emit(pair("U", input.second().getUnknownProbability(), input.first()));
                    }

                    private Pair<Pair<String, String>, Double> pair(final String gender, final double probability,
                            final String category) {
                        return new Pair<Pair<String, String>, Double>(new Pair<>(category, gender), probability);
                    }
                }, categoryGenderToProbabilityType);

        final PTable<Pair<String, String>, Double> y = x.groupByKey().combineValues(Aggregators.SUM_DOUBLES());

        final PTable<String, Pair<Integer, Double>> y2 = y.parallelDo(
                new DoFn<Pair<Pair<String, String>, Double>, Pair<String, Pair<Integer, Double>>>() {
                    private static final long serialVersionUID = 1121312312323123423L;

                    @Override
                    public void process(final Pair<Pair<String, String>, Double> input,
                            final Emitter<Pair<String, Pair<Integer, Double>>> emitter) {
                        emitter.emit(new Pair<String, Pair<Integer, Double>>(input.first().second(), new Pair<>(
                                Category.categoryOrder.get(input.first().first()), input.second())));
                    }

                }, genderToCategoryProbabilityType);

        final PTable<String, Collection<Double>> y3 = y2.parallelDo(
                new DoFn<Pair<String, Pair<Integer, Double>>, Pair<String, Collection<Double>>>() {

                    private static final long serialVersionUID = 112311341412392839L;

                    @Override
                    public void process(final Pair<String, Pair<Integer, Double>> input,
                            final Emitter<Pair<String, Collection<Double>>> emitter) {
                        emitter.emit(new Pair<String, Collection<Double>>(input.first(), createCollection(input
                                .second())));
                    }

                    private Collection<Double> createCollection(final Pair<Integer, Double> freq) {
                        final Double frequency[] = new Double[Category.categoryOrder.size()];
                        frequency[freq.first()] = freq.second();
                        return Arrays.asList(frequency);
                    }

                }, tableOfStringToDoubles);

        final PTable<String, Collection<Double>> y4 = y3.groupByKey().combineValues(
                new CombineFn<String, Collection<Double>>() {
                    private static final long serialVersionUID = 8834826647974920164L;

                    @Override
                    public void process(final Pair<String, Iterable<Collection<Double>>> input,
                            final Emitter<Pair<String, Collection<Double>>> emitter) {
                        final List<Double> sum = new ArrayList<>(Category.categoryOrder.size());
                        for (int i = 0; i < Category.categoryOrder.size(); i++) {
                            sum.add(0.0);
                        }

                        int idx = 0;
                        for (final Collection<Double> frequencies : input.second()) {
                            for (final Double frequency : frequencies) {
                                if (frequency != null) {
                                    sum.set(idx, sum.get(idx) + frequency);
                                }
                            }
                            idx++;
                        }
                        emitter.emit(new Pair<String, Collection<Double>>(input.first(), sum));
                    }
                });

        return y4;
    }
}
