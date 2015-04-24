package genderclassification.domain;

import com.google.common.collect.ImmutableMap;

public class NBModel {
    private final ImmutableMap<String, Double> male;
    private final ImmutableMap<String, Double> female;

    public NBModel(final ImmutableMap<String, Double> posteriorMale, final ImmutableMap<String, Double> posteriorFemale) {
        this.male = posteriorMale;
        this.female = posteriorFemale;
    }

    public Double getCategoryProbGivenMale(final String category) {
        return male.get(category);
    }

    public Double getCategoryProbGivenFemale(final String category) {
        return female.get(category);
    }
}
