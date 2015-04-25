package genderclassification.run;

import genderclassification.algorithm.cosinedistance.CosineDistanceClassification;
import genderclassification.algorithm.naivebayesian.NaiveBayesianClassification;
import genderclassification.pipeline.AbstractPipelineAdapter;
import genderclassification.pipeline.MemPipelineAdapter;

import java.io.IOException;

import org.apache.crunch.Pipeline;

public class Main {
    private static final ClassificationAlgorithm CLASSIFICATION_ALGORITHM = new CosineDistanceClassification();
	private static final MemPipelineAdapter pipelineAdapter = MemPipelineAdapter.getInstance();
	private static final int SEED = 57138921;

	public static void main(String[] args) throws IOException {
        CrossValidation crossValidation = new CrossValidation(SEED);
        double percentage = crossValidation.performCrossValidation(CLASSIFICATION_ALGORITHM) * 100;
        System.out.println("Score: " + percentage + "%");
    }

	public static Pipeline getPipeline() {
		return pipelineAdapter.getPipeline();
	}

	public static AbstractPipelineAdapter getAdapter() {
		return pipelineAdapter;
	}
}
