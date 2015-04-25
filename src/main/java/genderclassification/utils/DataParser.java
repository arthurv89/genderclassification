package genderclassification.utils;

import genderclassification.run.Main;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

public class DataParser {
    public static final File OUTPUT_FOLDER = new File("output/");
    public static final String OUTPUT_FOLDER_CLASSIFY = "output/classify/";
    public static final File OUTPUT_FOLDER_MODEL = new File("output/model/");

    private static final String INPUT_FILE_USER_PRODUCT = "input/user_product_add_2.txt";
    private static final String INPUT_FILE_USER_GENDER = "input/new_userId_gender.txt";
    private static final String INPUT_FILE_PRODUCT_CATEGORY = "input/product_to_category_lv2.txt";
    private static final String INPUT_FILE_CATEGORIES = "input/distinct_category.txt";

    private static final PCollection<String> userProductLines = Main.getPipeline().readTextFile(INPUT_FILE_USER_PRODUCT);
    private static final PCollection<String> userGenderLines = Main.getPipeline().readTextFile(INPUT_FILE_USER_GENDER);
    private static final PCollection<String> productCategoryLines = Main.getPipeline().readTextFile(INPUT_FILE_PRODUCT_CATEGORY);
    private static final PCollection<String> categoryLines = Main.getPipeline().readTextFile(INPUT_FILE_CATEGORIES);

    static {
        {
            Preconditions.checkArgument(new File(DataParser.INPUT_FILE_USER_PRODUCT).exists());
            Preconditions.checkArgument(new File(DataParser.INPUT_FILE_USER_GENDER).exists());
            Preconditions.checkArgument(new File(DataParser.INPUT_FILE_PRODUCT_CATEGORY).exists());
            Preconditions.checkArgument(new File(DataParser.INPUT_FILE_CATEGORIES).exists());
        }
    }

    public static List<String> parseCategories() throws IOException {
        File file = new File(INPUT_FILE_CATEGORIES);
        String fileContents = FileUtils.readFileToString(file);
        ImmutableList<String> categories = ImmutableList.copyOf(Splitter.on("\n").trimResults().split(fileContents));
        return categories;
    }

    public static final PCollection<String> classifiedUsers() {
        try {
            return Main.getPipeline().readTextFile(OUTPUT_FOLDER_CLASSIFY);
        } catch (Exception e) {
            return Main.getPipeline().emptyPCollection(DataTypes.STRING_TYPE);
        }
    }

    public final static PTable<String, String> productUser() {
        // (P,U)
        return userProductLines.parallelDo(new MapFn<String, Pair<String, String>>() {
            private static final long serialVersionUID = 5368118058771709696L;

            @Override
            public Pair<String, String> map(final String line) {
                final String[] values = line.split("\t");
                final String userId = values[0];
                final String productId = values[1];
                return new Pair<String, String>(productId, userId);
            }
        }, DataTypes.STRING_TO_STRING_TABLE_TYPE);
    }

    public final static PTable<String, String> productCategory() {
        // (P,C)
        return productCategoryLines.parallelDo(new MapFn<String, Pair<String, String>>() {
            private static final long serialVersionUID = 8685387020655952971L;

            @Override
            public Pair<String, String> map(final String line) {
                final String[] values = line.split("\t");
                final String productId = values[0];
                final String category = values[1];
                return new Pair<String, String>(productId, category);
            }
        }, DataTypes.STRING_TO_STRING_TABLE_TYPE);
    }

    private static boolean userGenderRead = false;
    public final static PTable<String, String> userGender() {
    	if(userGenderRead) {
    		throw new RuntimeException("You can't read the userGender directly");
    	}
    	
    	userGenderRead = true;
        // (U,G)
        return userGenderLines.parallelDo(new MapFn<String, Pair<String, String>>() {
            private static final long serialVersionUID = 8685387120655952971L;

            @Override
            public Pair<String, String> map(final String line) {
                final String[] values = line.split("\t");
                final String userId = values[0];
                final String genderProbabilities = values[1];
                return new Pair<String, String>(userId, genderProbabilities);
            }
        }, DataTypes.STRING_TO_STRING_TABLE_TYPE);
    }

    public final static PTable<String, String> userProduct() {
        // (U,P)
        return userProductLines.parallelDo(new MapFn<String, Pair<String, String>>() {
            private static final long serialVersionUID = 4431093387533962416L;

            @Override
            public Pair<String, String> map(final String line) {
                final String[] values = line.split("\t");
                final String userId = values[0];
                final String productId = values[1];
                return new Pair<String, String>(userId, productId);
            }
        }, DataTypes.STRING_TO_STRING_TABLE_TYPE);
    }

    public final static PTable<String, Long> categoryProducts() {
        // (U,P)
        return categoryLines.parallelDo(new MapFn<String, Pair<String, Long>>() {
            private static final long serialVersionUID = 4431093387533962416L;

            @Override
            public Pair<String, Long> map(final String line) {
                return new Pair<String, Long>(line, (long) 0);
            }
        }, DataTypes.STRING_TO_LONG_TYPE);
    }

    public static PTable<String, String> classifiedUserGender() {
        // (U,G)
        return classifiedUsers().parallelDo(new MapFn<String, Pair<String, String>>() {
            private static final long serialVersionUID = 532879173812973289L;

            @Override
            public Pair<String, String> map(final String line) {
                final String[] values = line.split("\t");
                final String userId = values[0];
                final String probabilities = values[1];
                return new Pair<String, String>(userId, probabilities);
            }
        }, DataTypes.STRING_TO_STRING_TABLE_TYPE);
    }
}
