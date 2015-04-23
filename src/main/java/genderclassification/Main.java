package genderclassification;

import genderclassification.classify.ClassifyJob;
import genderclassification.createmodel.ModelJob;
import genderclassification.domain.CategoryOrder;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

public class Main {
    public static void main(String[] args) throws IOException {
    	CategoryOrder.setCategories(parseCategories());
        /*while (true) {
            ModelJob.runJob();
            ClassifyJob.runJob();
        }*/
    	
    	ModelJob.runJobNaiveBayes();
    }

	private static List<String> parseCategories() throws IOException {
		File file = new File("input/distinct_category.txt");
		String fileContents = FileUtils.readFileToString(file);
		ImmutableList<String> categories = ImmutableList.copyOf(Splitter.on("\n").trimResults().split(fileContents));
		return categories;
	}
}
