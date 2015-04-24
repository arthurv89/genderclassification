package genderclassification;

import genderclassification.createmodel.ModelJob;
import genderclassification.domain.CategoryOrder;
import genderclassification.utils.DataParser;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        CategoryOrder.setCategories(DataParser.parseCategories());
        /*
         * while (true) { ModelJob.runJob(); ClassifyJob.runJob(); }
         */

        ModelJob.runJobNaiveBayes();
    }
}
