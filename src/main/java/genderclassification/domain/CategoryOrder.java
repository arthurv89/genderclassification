package genderclassification.domain;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CategoryOrder {
    private static Logger log = LoggerFactory.getLogger(CategoryOrder.class);

    private static Map<String, Integer> categoryOrder;

    public static void setCategories(final List<String> categories) {
        categoryOrder = convertCategories(categories);
        log.info("Initialized categories: " + categories);
    }

    private static Map<String, Integer> convertCategories(final List<String> categories) {
        final LinkedHashMap<String, Integer> map = new LinkedHashMap<String, Integer>();

        for (int i = 0; i < categories.size(); i++) {
            map.put(categories.get(i), i);
        }
        return map;
    }

    public static int getIndex(final String category) {
        final int idx = categoryOrder.get(category);
        return idx;
    }

    public static int countCategories() {
        return categoryOrder.size();
    }

    public static Map<String, Integer> getCategories() {
        return categoryOrder;
    }
}
