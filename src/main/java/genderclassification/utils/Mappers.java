package genderclassification.utils;

import org.apache.crunch.Pair;
import org.apache.crunch.fn.IdentityFn;

public class Mappers {
    public static final IdentityFn<Pair<String, String>> IDENTITY = IdentityFn.<Pair<String, String>> getInstance();
}
