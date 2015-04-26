package genderclassification.algorithm.cosinedistance;

import genderclassification.domain.CategoryOrder;
import genderclassification.domain.Model;
import genderclassification.pipeline.AbstractPipelineAdapter;
import genderclassification.pipeline.MemPipelineAdapter;
import genderclassification.run.ClassificationAlgorithm;
import genderclassification.run.Main;
import genderclassification.utils.ClassifyJob;
import genderclassification.utils.DataParser;
import genderclassification.utils.DataTypes;
import genderclassification.utils.ModelJobs;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;

import com.google.common.collect.Lists;

public class CosineDistanceClassification extends ClassificationAlgorithm {
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
        return finalize(classify);
    }

    private PTable<String, String> finalize(final PTable<String, String> classifiedUsers) {
        return classifiedUsers.parallelDo(chooseGender, DataTypes.STRING_TO_STRING_TABLE_TYPE);
    }

    private void createModel(final PTable<String, String> trainingDataset) throws IOException {
        FileUtils.deleteDirectory(ModelJobs.OUTPUT_FOLDER_MODEL);

        final MemPipelineAdapter adapter = MemPipelineAdapter.getInstance();
        final File outputFolder = adapter.performPipeline(pipeline -> {
            // (G, [freq])
                return CosineDistanceModel.determineModel(trainingDataset);
            }, ModelJobs.OUTPUT_FOLDER);

        ModelJobs.cleanupFiles(outputFolder);

        ModelJobs.printModel(adapter);
    }

    private PTable<String, String> classify(final PCollection<String> userIds) throws IOException {
        final AbstractPipelineAdapter adapter = Main.getAdapter();
        final List<String> lines = adapter.parseResult(DataParser.OUTPUT_FOLDER_MODEL);

        final Model model = ModelJobs.createModel(lines);

        final CosineDistanceClassifier classifier = new CosineDistanceClassifier(model);
        final PTable<String, String> lazyResults = classifier.classifyUsers(userIds);

        final File outputFolder = adapter.performPipeline(pipeline -> {
            return lazyResults;
        }, DataParser.OUTPUT_FOLDER);

        ClassifyJob.cleanupFiles(outputFolder);

        ClassifyJob.printResults(adapter);

        return lazyResults;
    }

    private final DoFn<Pair<String, String>, Pair<String, String>> chooseGender = new DoFn<Pair<String, String>, Pair<String, String>>() {
        private static final long serialVersionUID = 310669319740768120L;

        @Override
        public void process(final Pair<String, String> input, final Emitter<Pair<String, String>> emitter) {
            final String[] genderProbabilities = input.second().split(" ");
            final List<Double> genders = Lists.transform(Arrays.asList(genderProbabilities),
                    (final String s) -> Double.parseDouble(s));
            if (genders.get(0) > 0.5) {
                emitter.emit(new Pair<String, String>(input.first(), "1 0 0"));
            } else {
                emitter.emit(new Pair<String, String>(input.first(), "0 1 0"));
            }

        }
    };
}
