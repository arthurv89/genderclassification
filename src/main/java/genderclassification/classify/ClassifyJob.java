package genderclassification.classify;

import genderclassification.domain.Model;
import genderclassification.pipeline.AbstractPipelineAdapter;
import genderclassification.pipeline.MemPipelineAdapter;
import genderclassification.utils.DataParser;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pair;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class ClassifyJob {
    public static void runJob() throws IOException {
        final AbstractPipelineAdapter adapter = MemPipelineAdapter.getInstance();
        final List<String> lines = adapter.parseResult(DataParser.OUTPUT_FOLDER_MODEL);

        final Model model = createModel(lines);

        final Classifier classifier = new Classifier(model);
        final File outputFolder = adapter.performPipeline(pipeline -> {
            final PCollection<String> userProductLines = DataParser.userProductData(pipeline);
            final PCollection<String> userGenderLines = DataParser.userGenderData(pipeline);
            final PCollection<String> productCategoryLines = DataParser.productCategoryData(pipeline);
         
            return classifier.classifyUsers(userProductLines, userGenderLines, productCategoryLines);
        }, DataParser.OUTPUT_FOLDER);

        cleanupFiles(outputFolder);

        printResults(adapter);
    }

    private static void cleanupFiles(final File outputFolder) throws IOException {
        FileUtils.deleteDirectory(new File(DataParser.OUTPUT_FOLDER_CLASSIFY));
        FileUtils.moveDirectory(outputFolder, new File(DataParser.OUTPUT_FOLDER_CLASSIFY));
    }

    private static void printResults(final AbstractPipelineAdapter adapter) throws IOException {
        final List<String> classifiedUsers = adapter.parseResult(new File(DataParser.OUTPUT_FOLDER_CLASSIFY));

        System.out.println();
        System.out.println();
        System.out.println("The classification:");
        classifiedUsers.forEach(line -> {
            final String[] x = line.split("\\s");
            System.out.print(x[0] + "\t");
            System.out.print(Math.round(100 * Double.parseDouble(x[1])) + "%\t");
            System.out.print(Math.round(100 * Double.parseDouble(x[2])) + "%\t");
            System.out.print(Math.round(100 * Double.parseDouble(x[3])) + "%");
            System.out.println();
        });
    }

    public static Model createModel(final List<String> lines) {
        final Pair<String, List<Double>> g1 = split(lines.get(0));
        final Pair<String, List<Double>> g2 = split(lines.get(1));
        final Pair<String, List<Double>> g3 = split(lines.get(2));
        return new Model(ImmutableMap.<String, List<Double>> builder().put(g1.first(), g1.second())
                .put(g2.first(), g2.second()).put(g3.first(), g3.second()).build());
    }

    private static Pair<String, List<Double>> split(final String line) {
        final String[] genderAndFrequencies = line.split("\t");
        final String gender = genderAndFrequencies[0];
        final String[] freq = genderAndFrequencies[1].replace("[", "").replace("]", "").split(",");
        final List<Double> frequencies = Lists.transform(Arrays.asList(freq), s -> Double.parseDouble(s));
        return new Pair<String, List<Double>>(gender, frequencies);
    }
}
