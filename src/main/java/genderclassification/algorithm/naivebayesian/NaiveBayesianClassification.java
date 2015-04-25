package genderclassification.algorithm.naivebayesian;

import genderclassification.pipeline.AbstractPipelineAdapter;
import genderclassification.pipeline.MemPipelineAdapter;
import genderclassification.run.ClassificationAlgorithm;
import genderclassification.utils.DataParser;
import genderclassification.utils.ModelJobs;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class NaiveBayesianClassification extends ClassificationAlgorithm {
	@Override
	public PTable<String, String> run(final PTable<String, String> trainingDataset, final PCollection<String> userIds) {
		try {
			createModel(trainingDataset);
			//TO DO NBClassify -- not yet
			return classify(userIds);
		} catch(final IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void createModel(final PTable<String, String> trainingDataset) throws IOException {
        FileUtils.deleteDirectory(ModelJobs.OUTPUT_FOLDER_MODEL);

        final File outputFolder = MemPipelineAdapter.getInstance().performPipeline(pipeline -> {
            return NaiveBayesianGenderModel.determineModel(trainingDataset);
        }, ModelJobs.OUTPUT_FOLDER);

        ModelJobs.writePrior(Arrays.asList(NaiveBayesianGenderModel.getPriorMale(), NaiveBayesianGenderModel.getPriorFemale()));
        ModelJobs.cleanupFiles(outputFolder);
    }

    private PTable<String, String> classify(final PCollection<String> userIds) throws IOException {
        final AbstractPipelineAdapter adapter = MemPipelineAdapter.getInstance();
        final List<String> lines = adapter.parseResult(DataParser.OUTPUT_FOLDER_MODEL);

        final NaiveBayesianModel nbModel = readNBModel(lines);
        //TO DO: classifier model!
/*        final NBClassifier classifier = new NBClassifier(model);
        final File outputFolder = adapter.performPipeline(pipeline -> {
            final PCollection<String> userProductLines = DataParser.userProductData(pipeline);
            final PCollection<String> userGenderLines = DataParser.userGenderData(pipeline);
            final PCollection<String> productCategoryLines = DataParser.productCategoryData(pipeline);

            return classifier.classifyUsers(userProductLines, userGenderLines, productCategoryLines);
        }, DataParser.OUTPUT_FOLDER);

        cleanupFiles(outputFolder);

        printResults(adapter);*/
		return null;
    }

    private NaiveBayesianModel readNBModel(final List<String> lines) {
        final HashMap<String, List<Double>> map = new HashMap<String, List<Double>>();
        for (final String line : lines) {
            final Pair<String, List<Double>> pair = splitNb(line);
            map.put(pair.first(),pair.second());
        }
        ImmutableMap.<String, List<Double>>  builder().build();
        return new NaiveBayesianModel(ImmutableMap.copyOf(map));
    }
    
    private static Pair<String, List<Double>> splitNb(final String line) {
        final String[] categoryFreq = line.split("\t");
        final String category = categoryFreq[0];
        final String[] freq = categoryFreq[1].replace("[", "").replace("]", "").split(",");
        final List<Double> frequencies = Lists.transform(Arrays.asList(freq), s -> Double.parseDouble(s));
        return new Pair<String, List<Double>>(category, frequencies);
    }
}
