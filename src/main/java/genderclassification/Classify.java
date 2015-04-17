package genderclassification;

import genderclassification.model.ClassifiedUser;
import genderclassification.model.GenderProbabilities;

import java.io.Serializable;

import org.apache.crunch.DoFn;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.fn.IdentityFn;
import org.apache.crunch.lib.join.DefaultJoinStrategy;
import org.apache.crunch.lib.join.JoinType;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.Avros;

public class Classify implements Serializable {

	private static final long serialVersionUID = -6683089099880990848L;

	private static final IdentityFn<Pair<String, String>> IDENTITY = IdentityFn.<Pair<String,String>>getInstance();
	private static final AvroType<String> STRING_TYPE = Avros.records(String.class);
	private static final AvroType<GenderProbabilities> GENDER_PROBABILITY_TYPE = Avros.records(GenderProbabilities.class);
	
	private static final PTableType<String, String> STRING_TABLE = Avros.keyValueTableOf(STRING_TYPE, STRING_TYPE);
	private static final PTableType<String, GenderProbabilities> GENDER_PROBABILITY_TABLE = Avros.keyValueTableOf(STRING_TYPE, GENDER_PROBABILITY_TYPE);

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

	public static PCollection<ClassifiedUser> classifyUsers(final PCollection<String> userProductLines, final PCollection<String> userGenderLines, final PCollection<String> productCategoryLines) {
		final PTable<String, String> productToUser = userProductLines.parallelDo(parseUserProductLine, STRING_TABLE);
		final PTable<String, String> productToCategory = productCategoryLines.parallelDo(parseProductCategoryLine, STRING_TABLE);
		final PTable<String, GenderProbabilities> userToGender = userGenderLines.parallelDo(parseUserGenderLine, GENDER_PROBABILITY_TABLE);
		
		final PGroupedTable<String, String> userToCategories = new DefaultJoinStrategy<String, String, String>()
				.join(productToUser, productToCategory, JoinType.INNER_JOIN)
				.values()
				.parallelDo(IDENTITY, STRING_TABLE)
				.groupByKey();
		
		userToCategories.combineValues)
		
		
//		new DefaultJoinStrategy<String, String, String>().join(userToGender, userToCategories, JoinType.INNER_JOIN)
//		userToCategories
		
		final PCollection<ClassifiedUser> classifiedUsers = null;
		return classifiedUsers;
	}
}
