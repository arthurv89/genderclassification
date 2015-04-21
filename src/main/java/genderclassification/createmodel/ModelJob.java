package genderclassification.createmodel;

import genderclassification.classify.ClassifyJob;
import genderclassification.domain.Model;
import genderclassification.pipeline.MemPipelineAdapter;
import genderclassification.utils.DataParser;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.crunch.PCollection;

public class ModelJob implements Serializable {
    private static final long serialVersionUID = 540472578017394764L;

    public static final File OUTPUT_FOLDER = new File("output/");
    public static final File OUTPUT_FOLDER_MODEL = new File("output/model/");

    public static void runJob() throws IOException {
        FileUtils.deleteDirectory(OUTPUT_FOLDER_MODEL);

        final MemPipelineAdapter adapter = MemPipelineAdapter.getInstance();
        final File outputFolder = adapter.performPipeline(pipeline -> {
            final PCollection<String> userProductLines = DataParser.userProductData(pipeline);
            final PCollection<String> userGenderLines = DataParser.userGenderData(pipeline);
            final PCollection<String> productCategoryLines = DataParser.productCategoryData(pipeline);
            final PCollection<String> classifiedUsers = DataParser.classifiedUsers(pipeline);

            // (G, [freq])
                return GenderModel.determineModel(userProductLines, userGenderLines, classifiedUsers,
                        productCategoryLines);
            }, OUTPUT_FOLDER);

        cleanupFiles(outputFolder);

        printModel(adapter);
    }

    private static void cleanupFiles(final File outputFolder) throws IOException {
        FileUtils.deleteDirectory(OUTPUT_FOLDER_MODEL);
        FileUtils.moveDirectory(outputFolder, OUTPUT_FOLDER_MODEL);
    }

    private static void printModel(final MemPipelineAdapter adapter) throws IOException {
        final List<String> lines = adapter.parseResult(OUTPUT_FOLDER_MODEL);

        System.out.println();
        System.out.println();
        System.out.println("The model:");
        final Model model = ClassifyJob.createModel(lines);
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
}
