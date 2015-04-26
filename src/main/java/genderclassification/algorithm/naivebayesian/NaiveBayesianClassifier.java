package genderclassification.algorithm.naivebayesian;

public class NaiveBayesianClassifier {
    private static NaiveBayesianModel model;

    public NaiveBayesianClassifier(NaiveBayesianModel modelin) {
        model = modelin;
    }

    public static NaiveBayesianModel getNaiveBayesianModel(){
        return model;
    }
}
