package genderclassification.classify;

import genderclassification.domain.Category;
import genderclassification.domain.Model;
import genderclassification.utils.DataParser;
import genderclassification.utils.DataTypes;
import genderclassification.utils.Mappers;

import java.util.HashMap;
import java.util.Map;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.FilterFn;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.lib.join.DefaultJoinStrategy;
import org.apache.crunch.lib.join.JoinType;

public class Classifier {
	private final Model model;
	private final Map<String, Double> modelLengthByGender;

	public Classifier(final Model model) {
		this.model = model;
		modelLengthByGender = modelLengthByGender(model);
	}

	// (U,[prob])
	public PTable<String, String> classifyUsers(final PCollection<String> userProductLines, final PCollection<String> userGenderLines, final PCollection<String> productCategoryLines) {
		// (U,P)
		final PTable<String, String> userToProduct = DataParser.userProduct(userProductLines);
		// (U,G)
		final PTable<String, String> userToGender = DataParser.userGender(userGenderLines);
		// (P,C)
		final PTable<String, String> productToCategory = DataParser.productCategory(productCategoryLines);
		
		final PTable<String, String> undefinedUsersToProduct = new DefaultJoinStrategy<String, String, String>()
			// (U,P)*  JOIN  (U,G) = (U,(P,G))*
			.join(userToProduct, userToGender, JoinType.LEFT_OUTER_JOIN)
			// (U,(P,null))
			.filter(nullGender)
			// (U,P)
			.parallelDo(toProduct, DataTypes.STRING_TO_STRING_TABLE_TYPE);
		
		final PTable<String, String> productToUser = undefinedUsersToProduct
			// (P,U)
			.parallelDo(inverse, DataTypes.STRING_TO_STRING_TABLE_TYPE);
		
		return new DefaultJoinStrategy<String, String, String>()
			// (P,U)  JOIN  (P,C) = (P,(U,C))
			.join(productToUser, productToCategory, JoinType.INNER_JOIN)
			// (U,C)
			.values()
			// (U,C)
			.parallelDo(Mappers.IDENTITY, DataTypes.STRING_TO_STRING_TABLE_TYPE)
			// (U,[C])
			.groupByKey()
			// (U,[G])
			.mapValues(classify, DataTypes.STRING_TYPE);
	}
	
	private static final int CATEGORY_COUNT = Category.countCategories();

	private static FilterFn<Pair<String, Pair<String, String>>> nullGender = new FilterFn<Pair<String, Pair<String, String>>>() {
		private static final long serialVersionUID = -4777324870934777661L;

		@Override
		public boolean accept(final Pair<String, Pair<String, String>> input) {
			return input.second().second() == null;
		}
	};
	
	private static DoFn<Pair<String,Pair<String,String>>,Pair<String,String>> toProduct = new DoFn<Pair<String,Pair<String,String>>,Pair<String,String>>() {
		private static final long serialVersionUID = 5901533239721780409L;

		@Override
		public void process(final Pair<String, Pair<String, String>> input, final Emitter<Pair<String, String>> emitter) {
			emitter.emit(new Pair<String, String>(input.first(), input.second().first()));
		}
		
	};

	private final MapFn<Iterable<String>, String>  classify = new MapFn<Iterable<String>, String>() {
		private static final long serialVersionUID = -5267767964697018397L;

		@Override
		public String map(final Iterable<String> categories) {
			final double[] categoryVector = new double[CATEGORY_COUNT];
			for (final String category : categories) {
				final int idx = Category.getIndex(category);
				categoryVector[idx] += 1;
			}

			final double maleDistance = cosineDistance("M", categoryVector);
			final double femaleDistance = cosineDistance("F", categoryVector);
			final double unknownDistance = cosineDistance("U", categoryVector);
			
			final double sum = maleDistance + femaleDistance + unknownDistance;
			
			return new StringBuilder()
				.append(maleDistance/sum)
				.append(' ')
				.append(femaleDistance/sum)
				.append(' ')
				.append(unknownDistance/sum)
				.toString();
		}

		private double cosineDistance(final String gender, final double[] categoryVector) {
			double dotProduct = 0;
			for (int i = 0; i < categoryVector.length; i++) {
				dotProduct += categoryVector[i] * model.get(gender).get(i);
			}
			final double length = length(categoryVector) * modelLengthByGender.get(gender);
			if(length == 0) {
				return 0;
			}
			final double distance = dotProduct / length;
			return distance;
		}
	};
	

	private static DoFn<Pair<String, String>, Pair<String, String>> inverse  = new DoFn<Pair<String, String>, Pair<String, String>>() {
		private static final long serialVersionUID = 5135185910581L;

		@Override
		public void process(final Pair<String, String> input, final Emitter<Pair<String, String>> emitter) {
			emitter.emit(new Pair<String, String>(input.second(), input.first()));
		}
	};

	private double length(final double[] vector) {
		double length = 0;
		for(final Double a : vector) {
			length += a*a;
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
		for(final Double a : model.get(gender)) {
			length += a*a;
		}
		return Math.sqrt(length);
	}



	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static void print(final PTable table, final String name) {
		System.out.println("Printing table " + name + ":");
		table.materialize().forEach(r -> {
			System.out.println(r);
		});
		System.out.println("\n\n");
	}
}
