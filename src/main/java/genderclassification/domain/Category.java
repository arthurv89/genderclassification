package genderclassification.domain;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Category {
    private static final List<String> categories = Arrays.asList("Boeken", "Muziek", "Dvd", "categorieen", "Computer",
            "Games", "Speelgoed", "Baby", "Elektronica", "Koken, Tafelen en Huishouden", "Mooi & Gezond", "Klussen",
            "Sport & Vrije tijd", "Wonen", "Dieren", "Tuin", "Tassen & Lederwaren", "Horloges & Accessoires");

    private static final Map<String, Integer> categoryOrder = convertCategories();

    private static Map<String, Integer> convertCategories() {
        final HashMap<String, Integer> map = new HashMap<String, Integer>();

        for (int i = 0; i < categories.size(); i++) {
            map.put(categories.get(i), i);
        }
        return map;
    }

    public static int getIndex(String category) {
        int idx = categoryOrder.get(category);
        return idx;
    }

    public static int countCategories() {
        return categoryOrder.size();
    }
}
