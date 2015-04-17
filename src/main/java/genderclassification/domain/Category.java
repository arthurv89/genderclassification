package genderclassification.domain;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Category {
    private final static List<String> categories = Arrays.asList(
        "BEAUTY",
        "ELECTRONICS",
        "BOOKS");
    
    public final static Map<String, Integer> categoryOrder = convertCategories();
    
    private static Map<String, Integer> convertCategories() {
        final HashMap<String, Integer> map = new HashMap<String, Integer>();
        
        for(int i=0; i<categories.size(); i++) {
            map.put(categories.get(i), i);
        }
        return map;
    }
}
