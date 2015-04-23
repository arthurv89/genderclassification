package genderclassification.createmodel;

import genderclassification.domain.CategoryOrder;
import genderclassification.utils.DataParser;
import genderclassification.utils.DataTypes;
import genderclassification.utils.Mappers;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.crunch.CombineFn;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.FilterFn;
import org.apache.crunch.GroupingOptions;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PObject;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.lib.join.DefaultJoinStrategy;
import org.apache.crunch.lib.join.JoinType;

import com.google.common.primitives.Doubles;

public class GenderModel implements Serializable {
    public static PTable<String, Long> determineModelNaiveBayes(final PCollection<String> userProductLines,
             final PCollection<String> userGenderLines, 
            final PCollection<String> productCategoryLines) {
        // Parse the data files
        final PTable<String, String> productToUser = DataParser.productUser(userProductLines);
        final PTable<String, String> userToGender = DataParser.userGender(userGenderLines);
        final PTable<String, String> productToCategory = DataParser.productCategory(productCategoryLines);
        //final PTable<String, String> classifiedUserToGender = DataParser.classifiedUserGender(classifiedUserLines);

        final PTable<String, String> userToGenderString = userToGender.parallelDo(new DoFn<Pair<String, String>, Pair<String, String>>(){

            private static final long serialVersionUID = 12312312323L;

            @Override
            public void process(Pair<String, String> input, Emitter<Pair<String, String>> emitter) {
                String[] gender = input.second().split(" ");
                String realGender = "U";
                if (gender[0].equalsIgnoreCase("1"))
                    realGender = "M";
                else if (gender[1].equalsIgnoreCase("1"))
                    realGender = "F";
                emitter.emit(new Pair<String, String>(input.first(),realGender));
            }
            
        },DataTypes.STRING_TO_STRING_TABLE_TYPE);
        
        final PTable<String, String> userToCategory = new DefaultJoinStrategy<String, String, String>()
        // (P,U)* JOIN (P,C) = (P, (U,C))*
                .join(productToUser, productToCategory, JoinType.INNER_JOIN)
                // (U,C)
                .values()
                // (U,C)
                .parallelDo(Mappers.IDENTITY, DataTypes.STRING_TO_STRING_TABLE_TYPE);

        // print(productToUser, "productToUser");
        // print(productToCategory, "productToCategory");
        // System.out.println(userToCategory);

      //  final PTable<String, String> allUsersToGender = userToGender.union(classifiedUserToGender);

        final PTable<String, Pair<String, String>> genderToCategory = new DefaultJoinStrategy<String, String, String>()
        // (U,G) JOIN (U,C) = (U,(G,C))
                .join(userToGenderString, userToCategory, JoinType.INNER_JOIN);

        // print(userToGender, "userToGender");
        // print(userToCategory, "userToCategory");
        
         final PTable<String, Long> maleFreqEachCategory = genderToCategory.filter(new FilterFn<Pair<String, Pair<String, String>>>(){

                private static final long serialVersionUID = 1231232340L;
    
                @Override
                public boolean accept(Pair<String, Pair<String, String>> input) {
                    return input.second().first() == "M";
                }
         })
         .parallelDo(new DoFn<Pair<String, Pair<String, String>>, Pair<String,Double>>(){

               private static final long serialVersionUID = 123123213123L;
    
                @Override
                public void process(Pair<String, Pair<String, String>> input, Emitter<Pair<String, Double>> emitter) {
                    emitter.emit(new Pair<String, Double>(input.second().second(),1.0));
                }
         }, DataTypes.STRING_TO_DOUBLE_TYPE)
         .count()
         .parallelDo(new DoFn<Pair<Pair<String, Double>, Long>, Pair<String,Long>>(){

             private static final long serialVersionUID = 123123213123L;
  
              @Override
              public void process(Pair<Pair<String, Double>, Long> input, Emitter<Pair<String, Long>> emitter) {
                  emitter.emit(new Pair<String, Long>(input.first().first(), input.second() ));
              }
          }, DataTypes.STRING_TO_LONG_TYPE);
         
         
         
         
         System.out.println(maleFreqEachCategory);
         
         
        final PTable<String, Long> femaleFreqEachCategory = genderToCategory.filter(new FilterFn<Pair<String, Pair<String, String>>>(){

             private static final long serialVersionUID = 1231232340L;
 
             @Override
             public boolean accept(Pair<String, Pair<String, String>> input) {
                 return input.second().first() == "F";
             }
      })
      .parallelDo(new DoFn<Pair<String, Pair<String, String>>, Pair<String,Double>>(){

            private static final long serialVersionUID = 123123213123L;
 
             @Override
             public void process(Pair<String, Pair<String, String>> input, Emitter<Pair<String, Double>> emitter) {
                 emitter.emit(new Pair<String, Double>(input.second().second(),1.0));
             }
      }, DataTypes.STRING_TO_DOUBLE_TYPE)
      .count()
      .parallelDo(new DoFn<Pair<Pair<String, Double>, Long>, Pair<String,Long>>(){

          private static final long serialVersionUID = 123123213123L;

           @Override
           public void process(Pair<Pair<String, Double>, Long> input, Emitter<Pair<String, Long>> emitter) {
               emitter.emit(new Pair<String, Long>(input.first().first(), input.second() ));
           }
       }, DataTypes.STRING_TO_LONG_TYPE);
         
         
         System.out.println(femaleFreqEachCategory);

        //so far U gender is empty - so leave it for now
         
        //find the max between male and female
        final PTable<String, Long> maxMF = new DefaultJoinStrategy<String, Long, Long>()
        .join(maleFreqEachCategory, femaleFreqEachCategory, JoinType.FULL_OUTER_JOIN)
        .parallelDo(new DoFn<Pair<String,Pair<Long,Long>>,Pair<String, Long>>(){
            private static final long serialVersionUID = 123123213123L;

            @Override
            public void process(Pair<String, Pair<Long, Long>> input, Emitter<Pair<String, Long>> emitter) {
            	Long max = new Long(1);
            	if(input.second().first() == null) max = input.second().second();
            	else if (input.second().second() == null) max = input.second().first();
            	else max =  input.second().first() > input.second().second() ? input.second().first() : input.second().second();
            	
            	emitter.emit(new Pair<String, Long>(input.first(), max));
            }
        	
        }, DataTypes.STRING_TO_LONG_TYPE);
         
        System.out.println(maxMF);
        
        //compute IDF
        final PTable<String, Double> sumMF = new DefaultJoinStrategy<String, Long, Long>()
        .join(maleFreqEachCategory, femaleFreqEachCategory, JoinType.FULL_OUTER_JOIN)
        .parallelDo(new DoFn<Pair<String,Pair<Long,Long>>,Pair<String, Double>>(){
            private static final long serialVersionUID = 123123213123L;

            @Override
            public void process(Pair<String, Pair<Long, Long>> input, Emitter<Pair<String, Double>> emitter) {
            	Double sum = new Double(0);
            	if(input.second().first() != null) sum = sum + input.second().first();
            	if (input.second().second() != null) sum = sum + input.second().second();
            	emitter.emit(new Pair<String, Double>(input.first(), sum));
            }
        	
        }, DataTypes.STRING_TO_DOUBLE_TYPE);
        
        //to be done add idf        
        
        //compute TF for male
        final PTable<String, Double> tfMale = new DefaultJoinStrategy<String, Long, Long>()
                .join(maleFreqEachCategory, maxMF, JoinType.FULL_OUTER_JOIN)
                .parallelDo(new DoFn<Pair<String,Pair<Long,Long>>,Pair<String, Double>>(){
                    private static final long serialVersionUID = 123123213123L;

                    @Override
                    public void process(Pair<String, Pair<Long, Long>> input, Emitter<Pair<String, Double>> emitter) {
                    	Double freq = new Double(0);
                    	if(input.second().first() != null) freq = (double) ((0.5 * input.second().first()) / input.second().second());
                    	emitter.emit(new Pair<String, Double>(input.first(), freq + 0.5));
                    }
                	
                }, DataTypes.STRING_TO_DOUBLE_TYPE);
        
        System.out.println(tfMale);
        
        //compute TF for female
        final PTable<String, Double> tfFemale = new DefaultJoinStrategy<String, Long, Long>()
                .join(femaleFreqEachCategory, maxMF, JoinType.FULL_OUTER_JOIN)
                .parallelDo(new DoFn<Pair<String,Pair<Long,Long>>,Pair<String, Double>>(){
                    private static final long serialVersionUID = 123123213123L;

                    @Override
                    public void process(Pair<String, Pair<Long, Long>> input, Emitter<Pair<String, Double>> emitter) {
                    	Double freq = new Double(0);
                    	if(input.second().first() != null) freq = (double) ((0.5 * input.second().first()) / input.second().second());
                    	emitter.emit(new Pair<String, Double>(input.first(), freq + 0.5));
                    }
                	
                }, DataTypes.STRING_TO_DOUBLE_TYPE);
        
        System.out.println(tfFemale);
        
        
        //join with all available categories to create a full table
        
        
        return maleFreqEachCategory;
    }
    
    public static PTable<String, Collection<Double>> determineModel(final PCollection<String> userProductLines,
            final PCollection<String> userGenderLines, final PCollection<String> classifiedUserLines,
            final PCollection<String> productCategoryLines) {
        // Parse the data files
        final PTable<String, String> productToUser = DataParser.productUser(userProductLines);
        final PTable<String, String> userToGender = DataParser.userGender(userGenderLines);
        final PTable<String, String> productToCategory = DataParser.productCategory(productCategoryLines);
        final PTable<String, String> classifiedUserToGender = DataParser.classifiedUserGender(classifiedUserLines);

        final PTable<String, String> userToCategory = new DefaultJoinStrategy<String, String, String>()
        // (P,U)* JOIN (P,C) = (P, (U,C))*
                .join(productToUser, productToCategory, JoinType.INNER_JOIN)
                // (U,C)
                .values()
                // (U,C)
                .parallelDo(Mappers.IDENTITY, DataTypes.STRING_TO_STRING_TABLE_TYPE);

        // print(productToUser, "productToUser");
        // print(productToCategory, "productToCategory");
        // System.out.println(userToCategory);

        final PTable<String, String> allUsersToGender = userToGender.union(classifiedUserToGender);

        final PTable<String, Pair<String, String>> join = new DefaultJoinStrategy<String, String, String>()
        // (U,G) JOIN (U,C) = (U,(G,C))
                .join(allUsersToGender, userToCategory, JoinType.INNER_JOIN);

        // print(userToGender, "userToGender");
        // print(userToCategory, "userToCategory");
        // System.out.println(join);

        return join
        // (G,C)
                .values()
                // (GC,prob)*
                .parallelDo(toGenderAndCategoryPair_probability, DataTypes.PAIR_STRING_STRING_TO_DOUBLE_TABLE_TYPE)
                // (GC,[prob])
                .groupByKey()
                // (GC,prob)
                .combineValues(Aggregators.SUM_DOUBLES())
                // (GC,freq)*
                .parallelDo(toGender_frequencies, DataTypes.STRING_TO_DOUBLE_COLLECTION_TABLE_TYPE)
                // (G,[freq])
                .groupByKey()
                // (G, freqVector)
                .combineValues(sumFrequencies);
    }

    private static final long serialVersionUID = -6683089099880990848L;

    private static final int CATEGORY_COUNT = CategoryOrder.countCategories();

    private static final DoFn<Pair<String, String>, Pair<Pair<String, String>, Double>> toGenderAndCategoryPair_probability = new DoFn<Pair<String, String>, Pair<Pair<String, String>, Double>>() {
        private static final long serialVersionUID = 124672868421678412L;

        @Override
        public void process(final Pair<String, String> input, final Emitter<Pair<Pair<String, String>, Double>> emitter) {
            final String genderProbabilities = input.first();
            final String category = input.second();

            final String[] probabilityPerGender = genderProbabilities.split(" ");

            emitter.emit(pair(Gender.M, category, probabilityPerGender));
            emitter.emit(pair(Gender.F, category, probabilityPerGender));
            emitter.emit(pair(Gender.U, category, probabilityPerGender));
        }

        private Pair<Pair<String, String>, Double> pair(final Gender gender, final String category,
                final String[] probabilityPerGender) {
            final Pair<String, String> genderAndCategory = new Pair<>(gender.name(), category);
            final Double probability = Double.parseDouble(probabilityPerGender[gender.position]);
            return new Pair<Pair<String, String>, Double>(genderAndCategory, probability);
        }
    };

    private static final DoFn<Pair<Pair<String, String>, Double>, Pair<String, Collection<Double>>> toGender_frequencies = new DoFn<Pair<Pair<String, String>, Double>, Pair<String, Collection<Double>>>() {
        private static final long serialVersionUID = 1121312312323123423L;

        @Override
        public void process(final Pair<Pair<String, String>, Double> input,
                final Emitter<Pair<String, Collection<Double>>> emitter) {
            final String gender = input.first().first();
            final String category = input.first().second();
            final Double frequency = input.second();

            final List<Double> frequencies = createCollection(new Pair<>(CategoryOrder.getIndex(category), frequency));
            emitter.emit(new Pair<String, Collection<Double>>(gender, frequencies));
        }

        private List<Double> createCollection(final Pair<Integer, Double> categoryIndex_FrequencyPair) {
            final Integer categoryIndex = categoryIndex_FrequencyPair.first();
            final Double probability = categoryIndex_FrequencyPair.second();

            final double frequency[] = new double[CATEGORY_COUNT];
            frequency[categoryIndex] = probability;
            return Doubles.asList(frequency);
        }
    };

    private static CombineFn<String, Collection<Double>> sumFrequencies = new CombineFn<String, Collection<Double>>() {
        private static final long serialVersionUID = 8834826647974920164L;

        @Override
        public void process(final Pair<String, Iterable<Collection<Double>>> input,
                final Emitter<Pair<String, Collection<Double>>> emitter) {
            final String gender = input.first();
            final Iterable<Collection<Double>> frequenciesIterable = input.second();

            final double[] sum = new double[CATEGORY_COUNT];
            for (final Collection<Double> frequencies : frequenciesIterable) {
                final List<Double> frequencyList = (List<Double>) frequencies;
                for (int idx = 0; idx < frequencyList.size(); idx++) {
                    sum[idx] += frequencyList.get(idx);
                }
            }

            emitter.emit(new Pair<String, Collection<Double>>(gender, Doubles.asList(sum)));
        }
    };

    private enum Gender {
        M(0), F(1), U(2);

        private final int position;

        private Gender(final int position) {
            this.position = position;
        }
    }
}
