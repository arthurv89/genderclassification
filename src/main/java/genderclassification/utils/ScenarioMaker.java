package genderclassification.utils;

import java.util.Random;

import org.apache.crunch.FilterFn;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;

public class ScenarioMaker {

    public static PTable<String, String> selectOneOfNFemaleSamplesForEachClassForTrainingAndTesting(
            PTable<String, String> userToGender, int nSamplesRequired) {

        PTable<String, String> femaleSamples = userToGender.filter(new FilterFn<Pair<String, String>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public boolean accept(Pair<String, String> input) {
                Random rn = new Random();
                int answer = rn.nextInt(nSamplesRequired) + 1;
                return (input.second().equalsIgnoreCase("0 1 0") && answer == 2);
            }
        });

        PTable<String, String> maleSamples = userToGender.filter(new FilterFn<Pair<String, String>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public boolean accept(Pair<String, String> input) {
                return input.second().equalsIgnoreCase("1 0 0");
            }
        });

        return femaleSamples.union(maleSamples);
    }
}
