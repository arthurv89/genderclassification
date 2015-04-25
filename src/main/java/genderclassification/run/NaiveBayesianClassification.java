package genderclassification.run;

import genderclassification.classify.ClassifyJob;
import genderclassification.createmodel.ModelJob;

import java.io.IOException;

import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;

public class NaiveBayesianClassification extends ClassificationAlgorithm {
	@Override
	public PTable<String, String> run(final PTable<String, String> trainingDataset, final PCollection<String> userIds) {
		try {
			ModelJob.runJobNaiveBayes(trainingDataset);
			//TO DO NBClassify -- not yet
			return ClassifyJob.runJobNaiveBayes(userIds);
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
	}
}
