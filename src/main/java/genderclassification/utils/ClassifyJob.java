package genderclassification.utils;

import genderclassification.pipeline.AbstractPipelineAdapter;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;

public class ClassifyJob {
    public static void cleanupFiles(final File outputFolder) throws IOException {
        FileUtils.deleteDirectory(new File(DataParser.OUTPUT_FOLDER_CLASSIFY));
        FileUtils.moveDirectory(outputFolder, new File(DataParser.OUTPUT_FOLDER_CLASSIFY));
    }

    public static void printResults(final AbstractPipelineAdapter adapter) throws IOException {
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

}
