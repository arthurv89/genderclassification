package genderclassification.algorithm.naivebayesian;

import java.util.List;

import com.google.common.collect.ImmutableMap;

public class NBModel {
    private final ImmutableMap<String, List<Double>> both;
    public final static int MALE = 0; 
    public final static int FEMALE = 1; 

    public NBModel(final ImmutableMap<String, List<Double>> posteriorBoth) {
        this.both = posteriorBoth;
    }

    public List<Double> getCategoryProb(final String category) {
        return both.get(category);
    }

}
