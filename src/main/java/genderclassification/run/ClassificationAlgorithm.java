package genderclassification.run;

import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;

public abstract class ClassificationAlgorithm {
	public abstract PTable<String, String> run(final PTable<String, String> trainingDataset, final PCollection<String> testRowIds);
}
