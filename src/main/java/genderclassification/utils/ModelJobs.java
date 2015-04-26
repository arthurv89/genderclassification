package genderclassification.utils;

import genderclassification.domain.Model;
import genderclassification.pipeline.MemPipelineAdapter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.crunch.Pair;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class ModelJobs implements Serializable {
    private static final long serialVersionUID = 540472578017394764L;

    public static final File OUTPUT_FOLDER = new File("output/");
    public static final File OUTPUT_FOLDER_MODEL = new File("output/model/");

    public static void writePrior(List<Double> priors) throws IOException {
        try {
            File file = new File(OUTPUT_FOLDER_MODEL + "prior.txt");

            if (!file.exists()) {
                file.createNewFile();
            }

            FileWriter fw = new FileWriter(file.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(priors.toString());
            bw.close();
        } catch (IOException e) {
            throw e;
        }
    }

    public static void cleanupFiles(final File outputFolder) throws IOException {
        FileUtils.deleteDirectory(OUTPUT_FOLDER_MODEL);
        FileUtils.moveDirectory(outputFolder, OUTPUT_FOLDER_MODEL);
    }

    public static void printModel(final MemPipelineAdapter adapter) throws IOException {
        final List<String> lines = adapter.parseResult(OUTPUT_FOLDER_MODEL);

        System.out.println();
        System.out.println();
        System.out.println("The model:");
        final Model model = createModel(lines);
        printGender(model, "M");
        printGender(model, "F");
    }

    private static void printGender(final Model model, final String gender) {
        final double sum = model.get(gender).stream().mapToDouble(x -> x).sum();

        System.out.print(gender + "\t");
        model.get(gender).forEach(d -> {
            long freq = Math.round(d / sum * 100);
            System.out.print(freq + "%,\t");
        });
        System.out.println();
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
