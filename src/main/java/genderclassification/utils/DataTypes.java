package genderclassification.utils;

import java.util.Collection;

import org.apache.crunch.Pair;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.avro.AvroType;
import org.apache.crunch.types.avro.Avros;

public class DataTypes {
	public static final AvroType<String> STRING_TYPE = Avros.strings();
	public static final AvroType<Double> DOUBLE_TYPE = Avros.doubles();
	public static final AvroType<Long> LONG_TYPE = Avros.longs();
	public static final AvroType<Collection<Double>> DOUBLE_COLLECTION_TYPE = Avros.collections(Avros.doubles());
	public static final AvroType<Pair<String, String>> PAIR_STRING_STRING_TYPE = Avros.pairs(STRING_TYPE, STRING_TYPE);

	public static final PTableType<String, String> STRING_TO_STRING_TABLE_TYPE = Avros.keyValueTableOf(STRING_TYPE, STRING_TYPE);
	public static final PTableType<Pair<String, String>, Double> PAIR_STRING_STRING_TO_DOUBLE_TABLE_TYPE = Avros.keyValueTableOf(PAIR_STRING_STRING_TYPE, DOUBLE_TYPE);
	public static final PTableType<String, Collection<Double>> STRING_TO_DOUBLE_COLLECTION_TABLE_TYPE = Avros.keyValueTableOf(STRING_TYPE, DOUBLE_COLLECTION_TYPE);
	public static final PTableType<String, Double> STRING_TO_DOUBLE_TYPE = Avros.keyValueTableOf(STRING_TYPE, DOUBLE_TYPE);
	public static final PTableType<String, Long> STRING_TO_LONG_TYPE = Avros.keyValueTableOf(STRING_TYPE, LONG_TYPE);
}
