package genderclassification;

import genderclassification.domain.Category;

import java.io.Serializable;
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

import com.google.common.primitives.Doubles;

public class Classify implements Serializable {

	private enum Gender {
		M(0), F(1), U(2);
		
		private int position;

		private Gender(int position) {
			this.position = position;
		}
	}

	private static final int CATEGORY_COUNT = Category.countCategories();

	private static final long serialVersionUID = -6683089099880990848L;

	private static final AvroType<String> STRING_TYPE = Avros.strings();
	private static final AvroType<Double> DOUBLE_TYPE = Avros.doubles();
	private static final AvroType<Collection<Double>> DOUBLE_COLLECTION_TYPE = Avros.collections(Avros.doubles());
	private static final AvroType<Pair<String, String>> PAIR_STRING_STRING_TYPE = Avros.pairs(STRING_TYPE, STRING_TYPE);

	private static final PTableType<String, String> STRING_TO_STRING_TABLE_TYPE = Avros.keyValueTableOf(STRING_TYPE, STRING_TYPE);
	private static final PTableType<Pair<String, String>, Double> PAIR_STRING_STRING_TO_DOUBLE_TABLE_TYPE = Avros.keyValueTableOf(PAIR_STRING_STRING_TYPE, DOUBLE_TYPE);
	private static final PTableType<String, Collection<Double>> STRING_TO_DOUBLE_COLLECTION_TABLE_TYPE = Avros.keyValueTableOf(STRING_TYPE, DOUBLE_COLLECTION_TYPE);

	private static final IdentityFn<Pair<String, String>> IDENTITY = IdentityFn.<Pair<String, String>> getInstance();

	private static final MapFn<String, Pair<String, String>> parseUserProductLine = new MapFn<String, Pair<String, String>>() {
		private static final long serialVersionUID = 4431093387533962416L;

		@Override
		public Pair<String, String> map(final String line) {
			final String[] values = line.split("\t");
			final String userId = values[0];
			final String productId = values[1];
			return new Pair<String, String>(productId, userId);
		}
	};

	private static final MapFn<String, Pair<String, String>> parseProductCategoryLine = new MapFn<String, Pair<String, String>>() {
		private static final long serialVersionUID = 8685387020655952971L;

		@Override
		public Pair<String, String> map(final String line) {
			final String[] values = line.split("\t");
			final String productId = values[0];
			final String category = values[1];
			return new Pair<String, String>(productId, category);
		}
	};

	private static final DoFn<String, Pair<String, String>> parseUserGenderLine = new MapFn<String, Pair<String, String>>() {
		private static final long serialVersionUID = 8685387120655952971L;

		@Override
		public Pair<String, String> map(final String line) {
			final String[] values = line.split("\t");
			final String userId = values[0];
			final String genderProbabilities = values[1];
			return new Pair<String, String>(userId, genderProbabilities);
		}
	};

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

		private Pair<Pair<String, String>, Double> pair(final Gender gender, final String category, final String[] probabilityPerGender) {
			Pair<String, String> categoryAndGender = new Pair<>(category, gender.name());
			Double probability = Double.parseDouble(probabilityPerGender[gender.position]);
			return new Pair<Pair<String, String>, Double>(categoryAndGender, probability);
		}
	};

	private static final DoFn<Pair<Pair<String, String>, Double>, Pair<String, Collection<Double>>> toGender_frequencies = new DoFn<Pair<Pair<String, String>, Double>, Pair<String, Collection<Double>>>() {
		private static final long serialVersionUID = 1121312312323123423L;

		@Override
		public void process(final Pair<Pair<String, String>, Double> input, final Emitter<Pair<String, Collection<Double>>> emitter) {
			final String category = input.first().first();
			final String gender = input.first().second();
			final Double frequency = input.second();

			final List<Double> frequencies = createCollection(new Pair<>(Category.getIndex(category), frequency));
			emitter.emit(new Pair<String, Collection<Double>>(gender, frequencies));
		}

		private List<Double> createCollection(final Pair<Integer, Double> categoryIndex_FrequencyPair) {
			final Integer categoryIndex = categoryIndex_FrequencyPair.first();
			final Double probability = categoryIndex_FrequencyPair.second();

			final Double frequency[] = new Double[CATEGORY_COUNT];
			frequency[categoryIndex] = probability;
			return Arrays.asList(frequency);
		}
	};

	private static CombineFn<String, Collection<Double>> sumFrequencies = new CombineFn<String, Collection<Double>>() {
		private static final long serialVersionUID = 8834826647974920164L;

		@Override
		public void process(final Pair<String, Iterable<Collection<Double>>> input, final Emitter<Pair<String, Collection<Double>>> emitter) {
			final String category = input.first();
			final Iterable<Collection<Double>> frequenciesIterable = input.second();

			final double[] sum = new double[CATEGORY_COUNT];
			int idx = 0;
			for (final Collection<Double> frequencies : frequenciesIterable) {
				for (final Double frequency : frequencies) {
					if (frequency != null) {
						sum[idx] += frequency;
					}
				}
				idx++;
			}
			
			emitter.emit(new Pair<String, Collection<Double>>(category, Doubles.asList(sum)));
		}
	};

	public static PTable<String, Collection<Double>> determineModel(final PCollection<String> userProductLines, final PCollection<String> userGenderLines, final PCollection<String> productCategoryLines) {
		// Parse the data files
		// (P,U)
		final PTable<String, String> productToUser = userProductLines.parallelDo(parseUserProductLine, STRING_TO_STRING_TABLE_TYPE);
		// (P,C)
		final PTable<String, String> productToCategory = productCategoryLines.parallelDo(parseProductCategoryLine, STRING_TO_STRING_TABLE_TYPE);
		// (U,G)
		final PTable<String, String> userToGender = userGenderLines.parallelDo(parseUserGenderLine, STRING_TO_STRING_TABLE_TYPE);

		final PTable<String, String> userToCategory = new DefaultJoinStrategy<String, String, String>()
				// (P,U)*  JOIN  (P,C) = (P, (U,C))* 
				.join(productToUser, productToCategory, JoinType.INNER_JOIN)
				// (U,C)
				.values()
				// (U,C)
				.parallelDo(IDENTITY, STRING_TO_STRING_TABLE_TYPE);
		
//		print(productToUser, "productToUser");
//		print(productToCategory, "productToCategory");
//		print(userToCategory, "userToCategory");

		PTable<String, Pair<String, String>> join = new DefaultJoinStrategy<String, String, String>()
				// (U,G)  JOIN  (U,C) = (U,(G,C))
				.join(userToGender, userToCategory, JoinType.INNER_JOIN);

		print(userToGender, "userToGender");
		print(userToCategory, "userToCategory");
		print(join, "join");
		
		return join
				// (G,C)
				.values()
				// (GC,prob)*
				.parallelDo(toGenderAndCategoryPair_probability, PAIR_STRING_STRING_TO_DOUBLE_TABLE_TYPE)
				// (GC,[prob])
				.groupByKey()
				// (GC,prob)
				.combineValues(Aggregators.SUM_DOUBLES())
				// (GC,freq)*
				.parallelDo(toGender_frequencies, STRING_TO_DOUBLE_COLLECTION_TABLE_TYPE)
				// (G,[freq])
				.groupByKey()
				// (G, freqVector)
				.combineValues(sumFrequencies);
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
