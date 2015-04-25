package genderclassification.algorithm.cosinedistance;

import genderclassification.domain.CategoryOrder;
import genderclassification.domain.Model;
import genderclassification.utils.DataParser;
import genderclassification.utils.DataTypes;
import genderclassification.utils.Mappers;

import java.util.HashMap;
import java.util.Map;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.lib.join.DefaultJoinStrategy;
import org.apache.crunch.lib.join.JoinType;

public class CosineDistanceClassifier {
    private final Model model;
    private final Map<String, Double> modelLengthByGender;

    public CosineDistanceClassifier(final Model model) {
        this.model = model;
        modelLengthByGender = modelLengthByGender(model);
    }

    // (U,[prob])
    public PTable<String, String> classifyUsers(final PCollection<String> userIds) {
        // (U,P)
        final PTable<String, String> userToProduct = DataParser.userProduct();
        // (P,C)
        final PTable<String, String> productToCategory = DataParser.productCategory();

        final PTable<String, String> userToNull = userIds.parallelDo(userToNullMapper, DataTypes.STRING_TO_STRING_TABLE_TYPE);
        final PTable<String, String> productToUser = new DefaultJoinStrategy<String, String, String>()
        		// (U,P) JOIN (U,U) = (U,(P,U))
                .join(userToProduct, userToNull, JoinType.INNER_JOIN)
                // (U,P)
                .parallelDo(convertToUser_product, DataTypes.STRING_TO_STRING_TABLE_TYPE)
                // (P,U)
                .parallelDo(inverse, DataTypes.STRING_TO_STRING_TABLE_TYPE);

        PGroupedTable<String, String> userToCategory = new DefaultJoinStrategy<String, String, String>()
        // (P,U) JOIN (P,C) = (P,(U,C))
                .join(productToUser, productToCategory, JoinType.INNER_JOIN)
                // (U,C)
                .values()
                // (U,C)
                .parallelDo(Mappers.IDENTITY, DataTypes.STRING_TO_STRING_TABLE_TYPE)
                // (U,[C])
                .groupByKey();
        
        return userToCategory
                // (U,[G])
                .mapValues(classify, DataTypes.STRING_TYPE);
    }

    private static final int CATEGORY_COUNT = CategoryOrder.countCategories();

    private static DoFn<Pair<String, Pair<String, String>>, Pair<String, String>> convertToUser_product = new DoFn<Pair<String, Pair<String, String>>, Pair<String, String>>() {
        private static final long serialVersionUID = 5901533239721780409L;

        @Override
        public void process(final Pair<String, Pair<String, String>> input, final Emitter<Pair<String, String>> emitter) {
            emitter.emit(new Pair<String, String>(input.first(), input.second().first()));
        }

    };
    
    private final DoFn<String, Pair<String, String>> userToNullMapper = new DoFn<String, Pair<String, String>>() {
		private static final long serialVersionUID = 8189936866739388752L;

		@Override
		public void process(final String userId, final Emitter<Pair<String, String>> emitter) {
			emitter.emit(new Pair<String, String>(userId, null));
		}
	};

    private final MapFn<Iterable<String>, String> classify = new MapFn<Iterable<String>, String>() {
        private static final long serialVersionUID = -5267767964697018397L;

        @Override
        public String map(final Iterable<String> categories) {
            final double[] categoryVector = new double[CATEGORY_COUNT];
            for (final String category : categories) {
                final int idx = CategoryOrder.getIndex(category);
                categoryVector[idx] += 1;
            }

            final double maleDistance = cosineDistance("M", categoryVector);
            final double femaleDistance = cosineDistance("F", categoryVector);
            final double unknownDistance = cosineDistance("U", categoryVector);

            final double sum = maleDistance + femaleDistance + unknownDistance;

            final String probabilities = new StringBuilder()
            		.append(maleDistance / sum)
            		.append(' ')
                    .append(femaleDistance / sum)
                    .append(' ')
                    .append(unknownDistance / sum)
                    .toString();
            return probabilities;
        }

        private double cosineDistance(final String gender, final double[] categoryVector) {
            double dotProduct = 0;
            for (int i = 0; i < categoryVector.length; i++) {
                dotProduct += categoryVector[i] * model.get(gender).get(i);
            }
            final double length = length(categoryVector) * modelLengthByGender.get(gender);
            if (length == 0) {
                return 0;
            }
            final double distance = dotProduct / length;
            return distance;
        }
    };

    private static DoFn<Pair<String, String>, Pair<String, String>> inverse = new DoFn<Pair<String, String>, Pair<String, String>>() {
        private static final long serialVersionUID = 5135185910581L;

        @Override
        public void process(final Pair<String, String> input, final Emitter<Pair<String, String>> emitter) {
            emitter.emit(new Pair<String, String>(input.second(), input.first()));
        }
    };

    private double length(final double[] vector) {
        double length = 0;
        for (final Double a : vector) {
            length += a * a;
        }
        return Math.sqrt(length);
    }

    private Map<String, Double> modelLengthByGender(final Model model) {
        final Map<String, Double> map = new HashMap<>();
        map.put("M", modelLength("M"));
        map.put("F", modelLength("F"));
        map.put("U", modelLength("U"));
        return map;
    }

    private double modelLength(final String gender) {
        double length = 0;
        for (final Double a : model.get(gender)) {
            length += a * a;
        }
        return Math.sqrt(length);
    }
}
