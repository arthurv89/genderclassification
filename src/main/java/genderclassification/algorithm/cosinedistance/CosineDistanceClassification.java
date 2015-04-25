package genderclassification.algorithm.cosinedistance;

import genderclassification.domain.Model;
import genderclassification.pipeline.AbstractPipelineAdapter;
import genderclassification.pipeline.MemPipelineAdapter;
import genderclassification.run.ClassificationAlgorithm;
import genderclassification.run.Main;
import genderclassification.utils.ClassifyJob;
import genderclassification.utils.DataParser;
import genderclassification.utils.ModelJobs;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;

public class CosineDistanceClassification extends ClassificationAlgorithm {
	@Override
	public PTable<String, String> run(final PTable<String, String> trainingDataset, final PCollection<String> userIds) throws IOException {
		for (int i = 0; i < 10; i++) {
			createModel(trainingDataset);
			classify(userIds);
		}
		return null;
	}

	private void createModel(PTable<String, String> trainingDataset) throws IOException {
		FileUtils.deleteDirectory(ModelJobs.OUTPUT_FOLDER_MODEL);

		final MemPipelineAdapter adapter = MemPipelineAdapter.getInstance();
		final File outputFolder = adapter.performPipeline(pipeline -> {
			// (G, [freq])
				return CosineDistanceModel.determineModel(trainingDataset);
			}, ModelJobs.OUTPUT_FOLDER);

		ModelJobs.cleanupFiles(outputFolder);

		ModelJobs.printModel(adapter);
	}

	private void classify(PCollection<String> userIds) throws IOException {
        final AbstractPipelineAdapter adapter = Main.getAdapter();
        final List<String> lines = adapter.parseResult(DataParser.OUTPUT_FOLDER_MODEL);

        final Model model = ModelJobs.createModel(lines);

        final CosineDistanceClassifier classifier = new CosineDistanceClassifier(model);
        final File outputFolder = adapter.performPipeline(pipeline -> {
            return classifier.classifyUsers();
        }, DataParser.OUTPUT_FOLDER);

        ClassifyJob.cleanupFiles(outputFolder);

        ClassifyJob.printResults(adapter);
    }
}
