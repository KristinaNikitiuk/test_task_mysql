import de.daslaboratorium.machinelearning.classifier.Classifier;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

class DataTypeClassificator {

    private Classifier<String, String> bayes =
            new de.daslaboratorium.machinelearning.classifier.bayes.BayesClassifier<String, String>();

    /**
     * @param header
     *      Bayes classifier created to define column types dynamically !
     *      It could be used for more complex database scheme
     *      For example:
     *          field that contains part 'id' considered to be integer
     *
     *      Bayes model could be expanded and retrained for much more classes
     *
     *      FOR EXAMPLE  in our case we have 3 possible data types:
     *                  VARCHAR, DATE, INTEGER
     * @return
     */
    List<String> bayesClassification(List<String> header) {

        String[] number = "id country_id city_id".split("\\s");
        String[] varchar = "name text".split("\\s");
        String[] date = "date_of_birthday date day birthday".split("\\s");

        bayes.learn("INTEGER", Arrays.asList(number));
        bayes.learn("VARCHAR", Arrays.asList(varchar));
        bayes.learn("DATE", Arrays.asList(date));

        return header.stream().map(this :: classify_data_type).collect(Collectors.toList());

    }

     private String classify_data_type(String column_name) {
        String[] type = column_name.split("\\s");
        return bayes.classify(Arrays.asList(type)).getCategory();
    }
}
