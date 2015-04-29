package genderclassification.algorithm.naivebayesian;

import genderclassification.domain.CategoryOrder;
import genderclassification.pipeline.AbstractPipelineAdapter;
import genderclassification.pipeline.MemPipelineAdapter;
import genderclassification.run.ClassificationAlgorithm;
import genderclassification.utils.DataParser;
import genderclassification.utils.ModelJobs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
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
    private static final int ITERATIONS = 1;

    public void initialize() throws IOException {
        CategoryOrder.setCategories(DataParser.parseCategories());
    }

    @Override
    public PTable<String, String> run(final PTable<String, String> trainingDataset, final PCollection<String> userIds)
            throws IOException {
        PTable<String, String> classify = null;
        for (int i = 0; i < ITERATIONS; i++) {
            createModel(trainingDataset);
            classify = classify(userIds);
        }
        return classify;
    }

    private void createModel(final PTable<String, String> trainingDataset) throws IOException {
        FileUtils.deleteDirectory(ModelJobs.OUTPUT_FOLDER_MODEL);

        final File outputFolder = MemPipelineAdapter.getInstance().performPipeline(pipeline -> {
            return NaiveBayesianGenderModel.determineModel(trainingDataset);
        }, ModelJobs.OUTPUT_FOLDER);

        ModelJobs.writePrior(Arrays.asList(NaiveBayesianGenderModel.getPriorMale(),
                NaiveBayesianGenderModel.getPriorFemale()));
        ModelJobs.cleanupFiles(outputFolder);
    }

    private PTable<String, String> classify(final PCollection<String> userIds) throws IOException {
        final AbstractPipelineAdapter adapter = MemPipelineAdapter.getInstance();
        final List<String> lines = adapter.parseResult(DataParser.OUTPUT_FOLDER_MODEL);
        final String prior = readPrior();

        final NaiveBayesianModel NaiveBayesianModel = readNaiveBayesianModel(lines, prior);
        final NBClassifier classifier = new NBClassifier(NaiveBayesianModel);
        final PTable<String, String> lazyResults = classifier.classifyNaiveBayes(userIds);

        final File outputFolder = adapter.performPipeline(pipeline -> {
            return lazyResults;
        }, DataParser.OUTPUT_FOLDER);

        cleanupFiles(outputFolder);

        // printResults(adapter);
        return lazyResults;
    }

    private static void cleanupFiles(final File outputFolder) throws IOException {
        FileUtils.deleteDirectory(new File(DataParser.OUTPUT_FOLDER_CLASSIFY));
        FileUtils.moveDirectory(outputFolder, new File(DataParser.OUTPUT_FOLDER_CLASSIFY));
    }

    public static NaiveBayesianModel readNaiveBayesianModel(final List<String> lines, final String prior) {
        final HashMap<String, List<Double>> map = new HashMap<String, List<Double>>();
        for (String line : lines) {
            Pair<String, List<Double>> pair = splitNb(line);
            map.put(pair.first(), pair.second());
        }

        final HashMap<String, Double> mapPrior = new HashMap<String, Double>();
        final String[] priorString = prior.replace("[", "").replace("]", "").split(",");
        final List<Double> priorList = Lists.transform(Arrays.asList(priorString), s -> Double.parseDouble(s));
        mapPrior.put(NaiveBayesianModel.S_MALE, priorList.get(NaiveBayesianModel.MALE));
        mapPrior.put(NaiveBayesianModel.S_FEMALE, priorList.get(NaiveBayesianModel.FEMALE));

        ImmutableMap.<String, List<Double>> builder().build();
        return new NaiveBayesianModel(ImmutableMap.copyOf(map), ImmutableMap.copyOf(mapPrior));
    }

    private static Pair<String, List<Double>> splitNb(final String line) {
        final String[] categoryFreq = line.split("\t");
        final String category = categoryFreq[0];
        final String[] freq = categoryFreq[1].replace("[", "").replace("]", "").split(",");
        final List<Double> frequencies = Lists.transform(Arrays.asList(freq), s -> Double.parseDouble(s));
        return new Pair<String, List<Double>>(category, frequencies);
    }

    private static String readPrior() throws IOException {
        try {
            File file = new File(ModelJobs.OUTPUT_FOLDER_MODEL + "prior.txt");

            FileReader fr = new FileReader(file.getAbsoluteFile());
            BufferedReader br = new BufferedReader(fr);
            String prior = br.readLine();
            br.close();

            return prior;
        } catch (IOException e) {
            throw e;
        }
    }
}
