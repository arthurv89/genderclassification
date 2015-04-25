package genderclassification.domain;

import java.util.List;

import com.google.common.collect.ImmutableMap;

public class NBModel {
    private final ImmutableMap<String, List<Double>> both;
    private final ImmutableMap<String, Double> prior;

    public final static int MALE = 0;
    public final static int FEMALE = 1;

    public final static String S_MALE = "M";
    public final static String S_FEMALE = "F";

    public NBModel(final ImmutableMap<String, List<Double>> posteriorBoth, final ImmutableMap<String, Double> priorBoth) {
        this.both = posteriorBoth;
        this.prior = priorBoth;
    }

    public List<Double> getCategoryProb(final String category) {
        return both.get(category);
    }

    public Double getPrior(final String gender) {
        return prior.get(gender);
    }
}
