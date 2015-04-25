package genderclassification.algorithm.cosinedistance;

import genderclassification.classify.ClassifyJob;
import genderclassification.createmodel.GenderModel;
import genderclassification.createmodel.Jobs;
import genderclassification.pipeline.MemPipelineAdapter;
import genderclassification.run.ClassificationAlgorithm;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;

public class CosineDistanceClassification extends ClassificationAlgorithm {
	@Override
	public PTable<String, String> run(final PTable<String, String> trainingDataset, final PCollection<String> userIds) {
		try {
			createModel(trainingDataset);
			//TO DO NBClassify -- not yet
			return ClassifyJob.runJobNaiveBayes(userIds);
		} catch(final IOException e) {
			throw new RuntimeException(e);
		}
	}
	
    private void createModel(PTable<String, String> trainingDataset) throws IOException {
        FileUtils.deleteDirectory(Jobs.OUTPUT_FOLDER_MODEL);

        final MemPipelineAdapter adapter = MemPipelineAdapter.getInstance();
        final File outputFolder = adapter.performPipeline(pipeline -> {
            // (G, [freq])
            return GenderModel.determineModel(trainingDataset);
        }, Jobs.OUTPUT_FOLDER);

        Jobs.cleanupFiles(outputFolder);

        Jobs.printModel(adapter);
    }
}
