package genderclassification;

import genderclassification.pipeline.AbstractPipelineAdapter;
import genderclassification.pipeline.MemPipelineAdapter;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pipeline;

import com.google.common.base.Preconditions;

public class Main implements Serializable {
    private static final long serialVersionUID = 540472578017394764L;

    private static final String INPUT_USER_PRODUCT_FILE = "input/db_userId_productId.txt";
    private static final String INPUT_USER_GENDER_FILE = "input/db_userId_gender.txt";
    private static final String INPUT_PRODUCT_CATEGORY_FILE = "input/db_productId_category.txt";
    private static final String OUTPUT_FOLDER = "output";

    static {
        {
            Preconditions.checkArgument(new File(INPUT_USER_PRODUCT_FILE).exists());
            Preconditions.checkArgument(new File(INPUT_USER_GENDER_FILE).exists());
            Preconditions.checkArgument(new File(INPUT_PRODUCT_CATEGORY_FILE).exists());
        }
    }

    private static List<String> performPipeline(final AbstractPipelineAdapter pipelineAdapter) throws IOException {
        final File outputFolder = new File(OUTPUT_FOLDER, UUID.randomUUID().toString());

        final Pipeline pipeline = pipelineAdapter.getPipeline();
        pipeline.enableDebug();

        final PTable<String, Collection<Double>> bestMoveCollection = execute(pipeline);
        pipeline.writeTextFile(bestMoveCollection, outputFolder.getAbsolutePath());
        pipeline.done();

        final List<String> result = pipelineAdapter.parseResult(outputFolder);

        FileUtils.deleteDirectory(outputFolder);

        return result;
    }

	private static PTable<String, Collection<Double>> execute(final Pipeline pipeline) {
		final PCollection<String> userProductLines = pipeline.readTextFile(INPUT_USER_PRODUCT_FILE);
        final PCollection<String> userGenderLines = pipeline.readTextFile(INPUT_USER_GENDER_FILE);
        final PCollection<String> productCategoryLines = pipeline.readTextFile(INPUT_PRODUCT_CATEGORY_FILE);

        return Classify.determineModel(userProductLines, userGenderLines, productCategoryLines);
	}

    public static void main(final String[] args) throws IOException {
        final AbstractPipelineAdapter adapter = MemPipelineAdapter.getInstance();
        final List<String> result = performPipeline(adapter);
        System.out.println();
        System.out.println("The results:");
        System.out.println(result);
    }
}