package genderclassification.algorithm.naivebayesian;

<<<<<<< HEAD:src/main/java/genderclassification/createmodel/GenderModel.java
import genderclassification.domain.CategoryOrder;
import genderclassification.domain.NBModel;
=======
>>>>>>> dcf231563b83413e20a14647d794a610b9a72edd:src/main/java/genderclassification/algorithm/naivebayesian/NaiveBayesianGenderModel.java
import genderclassification.utils.DataParser;
import genderclassification.utils.DataTypes;
import genderclassification.utils.Mappers;

import java.io.Serializable;
import java.util.Collection;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.FilterFn;
import org.apache.crunch.PObject;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.lib.join.DefaultJoinStrategy;
import org.apache.crunch.lib.join.JoinType;

import static genderclassification.utils.MathFunctions.round;

import com.google.common.primitives.Doubles;

public class NaiveBayesianGenderModel implements Serializable {
	private static final long serialVersionUID = -258952378400958900L;
	
	private static PObject<Long> nrow;
    private static double priorM;
    private static double priorF;

    public static PTable<String, Collection<Double>> determineModel(PTable<String, String> userToGender) {
        // Parse the data files
<<<<<<< HEAD:src/main/java/genderclassification/createmodel/GenderModel.java
        final PTable<String, String> userToProduct = DataParser.productUser(userProductLines);
        final PTable<String, String> userToGender = DataParser.userGender(userGenderLines);
        final PTable<String, String> productToCategory = DataParser.productCategory(productCategoryLines);
        final PTable<String, Long> categories = DataParser.categoryProducts(categoryLines);
=======
        final PTable<String, String> productToUser = DataParser.productUser();
        final PTable<String, String> productToCategory = DataParser.productCategory();
        final PTable<String, Long> categories = DataParser.categoryProducts();
>>>>>>> dcf231563b83413e20a14647d794a610b9a72edd:src/main/java/genderclassification/algorithm/naivebayesian/NaiveBayesianGenderModel.java
        // final PTable<String, String> classifiedUserToGender = DataParser.classifiedUserGender(classifiedUserLines);

        final PTable<String, String> userToGenderString = userToGender.parallelDo(convertGenderToLetter,
                DataTypes.STRING_TO_STRING_TABLE_TYPE);

        nrow = userToGender.length(); // either #User or #Gender?
        priorM = round((double) userToGenderString.filter(filterGenderMale).length().getValue() / nrow.getValue(), 2);
        priorF = round((double) userToGenderString.filter(filterGenderFemale).length().getValue() / nrow.getValue(), 2);

        final PTable<String, String> userToCategory = new DefaultJoinStrategy<String, String, String>()
                .join(userToProduct, productToCategory, JoinType.INNER_JOIN).values()
                .parallelDo(Mappers.IDENTITY, DataTypes.STRING_TO_STRING_TABLE_TYPE);

        final PTable<String, Pair<String, String>> genderToCategory = new DefaultJoinStrategy<String, String, String>()
                .join(userToGenderString, userToCategory, JoinType.INNER_JOIN);

        @SuppressWarnings({ "unchecked", "rawtypes" })
        final PTable<String, Long> maleFreqEachCategory = new DefaultJoinStrategy()
                .join(genderToCategory.filter(filterMale).parallelDo(addOneToRecord, DataTypes.STRING_TO_DOUBLE_TYPE)
                        .count().parallelDo(selectFreqVal, DataTypes.STRING_TO_LONG_TYPE), categories,
                        JoinType.FULL_OUTER_JOIN).parallelDo(selectFreqValWithFrom2, DataTypes.STRING_TO_LONG_TYPE);

        System.out.println("Male Frequency: " + maleFreqEachCategory);

        @SuppressWarnings({ "unchecked", "rawtypes" })
        final PTable<String, Long> femaleFreqEachCategory = new DefaultJoinStrategy().join(
                genderToCategory.filter(filterFemale).parallelDo(addOneToRecord, DataTypes.STRING_TO_DOUBLE_TYPE)
                        .count().parallelDo(selectFreqVal, DataTypes.STRING_TO_LONG_TYPE), categories,
                JoinType.FULL_OUTER_JOIN).parallelDo(selectFreqValWithFrom2, DataTypes.STRING_TO_LONG_TYPE);

        System.out.println("Female Frequency: " + femaleFreqEachCategory);

        // so far U gender is empty - so leave it for now

        // find the max between male and female
        final PTable<String, Long> maxMF = new DefaultJoinStrategy<String, Long, Long>().join(maleFreqEachCategory,
                femaleFreqEachCategory, JoinType.FULL_OUTER_JOIN).parallelDo(selectMax, DataTypes.STRING_TO_LONG_TYPE);

        System.out.println("Max Frequency: " + maxMF);

        // compute IDF
        final PTable<String, Double> idf = new DefaultJoinStrategy<String, Long, Long>().join(maleFreqEachCategory,
                femaleFreqEachCategory, JoinType.FULL_OUTER_JOIN).parallelDo(doSum, DataTypes.STRING_TO_DOUBLE_TYPE);

        System.out.println("IDF: " + idf);

        // compute TF for male
        final PTable<String, Double> tfMale = new DefaultJoinStrategy<String, Long, Long>().join(maleFreqEachCategory,
                maxMF, JoinType.FULL_OUTER_JOIN).parallelDo(computeTf, DataTypes.STRING_TO_DOUBLE_TYPE);

        System.out.println("TF Male: " + tfMale);

        // compute TF for female
        final PTable<String, Double> tfFemale = new DefaultJoinStrategy<String, Long, Long>().join(
                femaleFreqEachCategory, maxMF, JoinType.FULL_OUTER_JOIN).parallelDo(computeTf,
                DataTypes.STRING_TO_DOUBLE_TYPE);

        System.out.println("TF Female: " + tfFemale);

        // join with all available categories to create a full table of TF-IDF
        final PTable<String, Double> tfidfMale = new DefaultJoinStrategy<String, Double, Double>().join(tfMale, idf,
                JoinType.FULL_OUTER_JOIN).parallelDo(computeTfIdf, DataTypes.STRING_TO_DOUBLE_TYPE);

        System.out.println("TF-IDF Male class: " + tfidfMale);

        final PTable<String, Double> tfidfFemale = new DefaultJoinStrategy<String, Double, Double>().join(tfFemale,
                idf, JoinType.FULL_OUTER_JOIN).parallelDo(computeTfIdf, DataTypes.STRING_TO_DOUBLE_TYPE);

        System.out.println("TF-IDF Female class: " + tfidfFemale);

        // join Male and Female tfidf into a table
        final PTable<String, Collection<Double>> tfidf = new DefaultJoinStrategy<String, Double, Double>().join(
                tfidfMale, tfidfFemale, JoinType.FULL_OUTER_JOIN).parallelDo(joinTableTfIdf,
                DataTypes.STRING_TO_DOUBLE_COLLECTION_TABLE_TYPE);

        System.out.println("TF-IDF Table: " + tfidf);

        // normalize tfidf table
        final PTable<String, Collection<Double>> tfidfNorm = tfidf.parallelDo(normalizeTfIdf,
                DataTypes.STRING_TO_DOUBLE_COLLECTION_TABLE_TYPE);

        System.out.println("Normalized TF-IDF Table: " + tfidfNorm);
        return tfidfNorm;
    }

<<<<<<< HEAD:src/main/java/genderclassification/createmodel/GenderModel.java
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

    public static double getPriorMale() {
        return priorM;
    }

    public static double getPriorFemale() {
        return priorF;
    }

    private static final long serialVersionUID = -6683089099880990848L;

    private static final int CATEGORY_COUNT = CategoryOrder.countCategories();

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

            final List<Double> frequencies = createCollection(new Pair<>(CategoryOrder.getIndex(category), frequency));
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

    public static DoFn<Pair<String, String>, Pair<String, String>> convertGenderToLetter = new DoFn<Pair<String, String>, Pair<String, String>>() {
=======
    private static DoFn<Pair<String, String>, Pair<String, String>> convertGenderToLetter = new DoFn<Pair<String, String>, Pair<String, String>>() {
>>>>>>> dcf231563b83413e20a14647d794a610b9a72edd:src/main/java/genderclassification/algorithm/naivebayesian/NaiveBayesianGenderModel.java

        private static final long serialVersionUID = 12312312323L;

        @Override
        public void process(Pair<String, String> input, Emitter<Pair<String, String>> emitter) {
            String[] gender = input.second().split(" ");
            String realGender = "U";
            if (gender[0].equalsIgnoreCase("1"))
                realGender = NBModel.S_MALE;
            else if (gender[1].equalsIgnoreCase("1"))
                realGender = NBModel.S_FEMALE;
            emitter.emit(new Pair<String, String>(input.first(), realGender));
        }
    };

    private static FilterFn<Pair<String, String>> filterGenderMale = new FilterFn<Pair<String, String>>() {

        private static final long serialVersionUID = 1231232340L;

        @Override
        public boolean accept(Pair<String, String> input) {
            return input.second() == NBModel.S_MALE;
        }
    };

    private static FilterFn<Pair<String, String>> filterGenderFemale = new FilterFn<Pair<String, String>>() {

        private static final long serialVersionUID = 1231232340L;

        @Override
        public boolean accept(Pair<String, String> input) {
            return input.second() == NBModel.S_FEMALE;
        }
    };

    private static FilterFn<Pair<String, Pair<String, String>>> filterMale = new FilterFn<Pair<String, Pair<String, String>>>() {

        private static final long serialVersionUID = 1231232340L;

        @Override
        public boolean accept(Pair<String, Pair<String, String>> input) {
            return input.second().first() == NBModel.S_MALE;
        }
    };

    private static FilterFn<Pair<String, Pair<String, String>>> filterFemale = new FilterFn<Pair<String, Pair<String, String>>>() {

        private static final long serialVersionUID = 1231232340L;

        @Override
        public boolean accept(Pair<String, Pair<String, String>> input) {
            return input.second().first() == NBModel.S_FEMALE;
        }
    };

    private static DoFn<Pair<String, Pair<String, String>>, Pair<String, Double>> addOneToRecord = new DoFn<Pair<String, Pair<String, String>>, Pair<String, Double>>() {

        private static final long serialVersionUID = 123123213123L;

        @Override
        public void process(Pair<String, Pair<String, String>> input, Emitter<Pair<String, Double>> emitter) {
            emitter.emit(new Pair<String, Double>(input.second().second(), 1.0));
        }
    };

    private static DoFn<Pair<Pair<String, Double>, Long>, Pair<String, Long>> selectFreqVal = new DoFn<Pair<Pair<String, Double>, Long>, Pair<String, Long>>() {

        private static final long serialVersionUID = 123123213123L;

        @Override
        public void process(Pair<Pair<String, Double>, Long> input, Emitter<Pair<String, Long>> emitter) {
            emitter.emit(new Pair<String, Long>(input.first().first(), input.second()));
        }
    };

    private static DoFn<Pair<String, Pair<Long, Long>>, Pair<String, Long>> selectFreqValWithFrom2 = new DoFn<Pair<String, Pair<Long, Long>>, Pair<String, Long>>() {

        private static final long serialVersionUID = 123123213123L;

        @Override
        public void process(Pair<String, Pair<Long, Long>> input, Emitter<Pair<String, Long>> emitter) {
            emitter.emit(new Pair<String, Long>(input.first(), input.second().first() != null ? input.second().first()
                    : input.second().second()));
        }

    };

    private static DoFn<Pair<String, Pair<Long, Long>>, Pair<String, Long>> selectMax = new DoFn<Pair<String, Pair<Long, Long>>, Pair<String, Long>>() {
        private static final long serialVersionUID = 123123213123L;

        @Override
        public void process(Pair<String, Pair<Long, Long>> input, Emitter<Pair<String, Long>> emitter) {
            Long max = new Long(1);
            if (input.second().first() == null)
                max = input.second().second();
            else if (input.second().second() == null)
                max = input.second().first();
            else
                max = input.second().first() > input.second().second() ? input.second().first() : input.second()
                        .second();

            emitter.emit(new Pair<String, Long>(input.first(), max));
        }

    };

    private static DoFn<Pair<String, Pair<Long, Long>>, Pair<String, Double>> doSum = new DoFn<Pair<String, Pair<Long, Long>>, Pair<String, Double>>() {
        private static final long serialVersionUID = 123123213123L;

        @Override
        public void process(Pair<String, Pair<Long, Long>> input, Emitter<Pair<String, Double>> emitter) {
            Double sum = new Double(0);
            if (input.second().first() != null)
                sum = sum + input.second().first();
            if (input.second().second() != null)
                sum = sum + input.second().second();
            emitter.emit(new Pair<String, Double>(input.first(), Math.log10(nrow.getValue() / sum)));
        }

    };

    private static DoFn<Pair<String, Pair<Long, Long>>, Pair<String, Double>> computeTf = new DoFn<Pair<String, Pair<Long, Long>>, Pair<String, Double>>() {
        private static final long serialVersionUID = 123123213123L;

        @Override
        public void process(Pair<String, Pair<Long, Long>> input, Emitter<Pair<String, Double>> emitter) {
            Double freq = new Double(0);
            if (input.second().first() != null)
                freq = (double) ((0.5 * input.second().first()) / input.second().second());
            emitter.emit(new Pair<String, Double>(input.first(), freq + 0.5));
        }

    };

    private static DoFn<Pair<String, Pair<Double, Double>>, Pair<String, Double>> computeTfIdf = new DoFn<Pair<String, Pair<Double, Double>>, Pair<String, Double>>() {

        private static final long serialVersionUID = 1123151L;

        @Override
        public void process(Pair<String, Pair<Double, Double>> input, Emitter<Pair<String, Double>> emitter) {
            emitter.emit(new Pair<String, Double>(input.first(), input.second().first() * input.second().second()));
        }

    };

    private static DoFn<Pair<String, Pair<Double, Double>>, Pair<String, Collection<Double>>> joinTableTfIdf = new DoFn<Pair<String, Pair<Double, Double>>, Pair<String, Collection<Double>>>() {

        private static final long serialVersionUID = 1L;

        @Override
        public void process(Pair<String, Pair<Double, Double>> input, Emitter<Pair<String, Collection<Double>>> emitter) {
            final double[] val = new double[2];
            val[0] = (input.second().first() != null ? input.second().first() : 0);
            val[1] = (input.second().second() != null ? input.second().second() : 0);
            emitter.emit(new Pair<String, Collection<Double>>(input.first(), Doubles.asList(val)));
        }

    };

    private static DoFn<Pair<String, Collection<Double>>, Pair<String, Collection<Double>>> normalizeTfIdf = new DoFn<Pair<String, Collection<Double>>, Pair<String, Collection<Double>>>() {

        private static final long serialVersionUID = 1231312412421L;

        @Override
        public void process(Pair<String, Collection<Double>> input, Emitter<Pair<String, Collection<Double>>> emitter) {
            double[] norm = Doubles.toArray(input.second());
            double denum = norm[0] + norm[1];
            norm[0] = round(norm[0] / denum, 2);
            norm[1] = round(norm[1] / denum, 2);
            emitter.emit(new Pair<String, Collection<Double>>(input.first(), Doubles.asList(norm)));
        }
    };

    public static double getPriorMale() {
        return priorM;
    }

    public static double getPriorFemale() {
        return priorF;
    }
}
