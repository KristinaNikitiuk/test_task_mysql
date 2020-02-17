import com.opencsv.CSVReader;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class Utils {

    static List<String> read_csv_header(String path) {

        List<String> arrlist = new ArrayList<String>();

        try {
            String firstLine = Files.lines(Paths.get(path)).findFirst().get();
            arrlist = Arrays.asList(firstLine.split("\\s*,\\s*"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return arrlist;
    }

    static StringBuilder read_csv(String path) {

        CSVReader reader = null;
        StringBuilder data = new StringBuilder();
        try {
            reader = new CSVReader(new FileReader(path));
            String[] line;

            int iteration = 0;
            while ((line = reader.readNext()) != null) {
                if(iteration == 0) {
                    iteration++;
                    continue;
                }

                data.append(Arrays.toString(line).replaceAll("\\[", "(")
                        .replaceAll("\\]","),"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return data;
    }

    static String getFileName(String filePath) {

        String filename =  new File(filePath).getName();
        if (filename.indexOf(".") > 0) {
            return filename.substring(0, filename.lastIndexOf("."));
        }
        else return filename;
    }

    static JSONObject parseConfigurations(String path){

        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = null;
        try (FileReader reader = new FileReader(path))
        {
            Object obj = jsonParser.parse(reader);
            jsonObject = (JSONObject) obj;

        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
        return jsonObject;
    }

    static String getColumnDataType(String header) {

        if (header.contains("id")) return "INTEGER";
        else if (header.contains("date")) return "DATE";
        else return "VARCHAR";

    }

}
