package genderclassification.algorithm.naivebayesian;

import java.util.List;

import com.google.common.collect.ImmutableMap;

<<<<<<< HEAD:src/main/java/genderclassification/domain/NBModel.java
public class NBModel {
    private final ImmutableMap<String, List<Double>> both;
    private final ImmutableMap<String, Double> prior;

    public final static int MALE = 0;
    public final static int FEMALE = 1;

    public final static String S_MALE = "M";
    public final static String S_FEMALE = "F";

    public NBModel(final ImmutableMap<String, List<Double>> posteriorBoth, final ImmutableMap<String, Double> priorBoth) {
=======
public class NaiveBayesianModel {
    private final ImmutableMap<String, List<Double>> both;
    public final static int MALE = 0; 
    public final static int FEMALE = 1; 

    public NaiveBayesianModel(final ImmutableMap<String, List<Double>> posteriorBoth) {
>>>>>>> dcf231563b83413e20a14647d794a610b9a72edd:src/main/java/genderclassification/algorithm/naivebayesian/NaiveBayesianModel.java
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
