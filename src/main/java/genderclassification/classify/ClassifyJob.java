package genderclassification.classify;

import genderclassification.createmodel.ModelJob;
import genderclassification.domain.Model;
import genderclassification.domain.NBModel;
import genderclassification.pipeline.AbstractPipelineAdapter;
import genderclassification.pipeline.MemPipelineAdapter;
import genderclassification.utils.DataParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pair;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class ClassifyJob {
    public static void runJobNaiveBayes() throws IOException {
        final AbstractPipelineAdapter adapter = MemPipelineAdapter.getInstance();
        final List<String> lines = adapter.parseResult(DataParser.OUTPUT_FOLDER_MODEL);
        final String prior = readPrior();

        final NBModel nbModel = readNBModel(lines, prior);
        final NBClassifier classifier = new NBClassifier(nbModel);
        // TO DO: read prior probability

        final File outputFolder = adapter.performPipeline(pipeline -> {
            final PCollection<String> userProductLines = DataParser.userProductData(pipeline);
            final PCollection<String> userGenderLines = DataParser.userGenderData(pipeline);
            final PCollection<String> productCategoryLines = DataParser.productCategoryData(pipeline);

            return classifier.classifyNaiveBayes(userProductLines, userGenderLines, productCategoryLines);
        }, DataParser.OUTPUT_FOLDER);

        cleanupFiles(outputFolder);

        // printResults(adapter);
    }

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

    public static NBModel readNBModel(final List<String> lines, final String prior) {
        final HashMap<String, List<Double>> map = new HashMap<String, List<Double>>();
        for (String line : lines) {
            Pair<String, List<Double>> pair = splitNb(line);
            map.put(pair.first(), pair.second());
        }

        final HashMap<String, Double> mapPrior = new HashMap<String, Double>();
        final String[] priorString = prior.replace("[", "").replace("]", "").split(",");
        final List<Double> priorList = Lists.transform(Arrays.asList(priorString), s -> Double.parseDouble(s));
        mapPrior.put(NBModel.S_MALE, priorList.get(NBModel.MALE));
        mapPrior.put(NBModel.S_FEMALE, priorList.get(NBModel.FEMALE));

        ImmutableMap.<String, List<Double>> builder().build();
        return new NBModel(ImmutableMap.copyOf(map), ImmutableMap.copyOf(mapPrior));
    }

    private static Pair<String, List<Double>> split(final String line) {
        final String[] genderAndFrequencies = line.split("\t");
        final String gender = genderAndFrequencies[0];
        final String[] freq = genderAndFrequencies[1].replace("[", "").replace("]", "").split(",");
        final List<Double> frequencies = Lists.transform(Arrays.asList(freq), s -> Double.parseDouble(s));
        return new Pair<String, List<Double>>(gender, frequencies);
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
            File file = new File(ModelJob.OUTPUT_FOLDER_MODEL + "prior.txt");

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
