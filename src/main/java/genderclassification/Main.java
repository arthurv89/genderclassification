package genderclassification;

import genderclassification.classify.ClassifyJob;
import genderclassification.createmodel.ModelJob;
import genderclassification.domain.CategoryOrder;
import genderclassification.utils.DataParser;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

public class Main {
    public static void main(String[] args) throws IOException {
    	CategoryOrder.setCategories(DataParser.parseCategories());
        /*while (true) {
            ModelJob.runJob();
            ClassifyJob.runJob();
        }*/
    	
    	ModelJob.runJobNaiveBayes();
    }
}
