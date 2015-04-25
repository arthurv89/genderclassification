package genderclassification.algorithm.naivebayesian;

import genderclassification.classify.ClassifyJob;
import genderclassification.createmodel.GenderModel;
import genderclassification.createmodel.Jobs;
import genderclassification.pipeline.MemPipelineAdapter;
import genderclassification.run.ClassificationAlgorithm;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;

public class NaiveBayesianClassification extends ClassificationAlgorithm {
	@Override
	public PTable<String, String> run(final PTable<String, String> trainingDataset, final PCollection<String> userIds) {
		try {
			runJobNaiveBayes(trainingDataset);
			//TO DO NBClassify -- not yet
			return ClassifyJob.runJobNaiveBayes(userIds);
		} catch(final IOException e) {
			throw new RuntimeException(e);
		}
	}

    public static void runJobNaiveBayes(PTable<String, String> trainingDataset) throws IOException {
        FileUtils.deleteDirectory(Jobs.OUTPUT_FOLDER_MODEL);

        final MemPipelineAdapter adapter = MemPipelineAdapter.getInstance();
        final File outputFolder = adapter.performPipeline(pipeline -> {
            return GenderModel.determineModel(trainingDataset);
        }, Jobs.OUTPUT_FOLDER);

        Jobs.writePrior(Arrays.asList(GenderModel.getPriorMale(), GenderModel.getPriorFemale()));
        Jobs.cleanupFiles(outputFolder);
    }
}
