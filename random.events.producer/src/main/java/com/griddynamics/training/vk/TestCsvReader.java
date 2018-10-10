package com.griddynamics.training.vk;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import java.io.FileReader;
import java.io.StringReader;

import static com.opencsv.ICSVParser.NULL_CHARACTER;

public class TestCsvReader {

    public static void main(String[] args) throws Exception {
        try (CSVReader reader = new CSVReaderBuilder(new StringReader("TRIX T23469 H0 Luggage wagon of K.BAY.STS.instance\\, Luggage van,36.51,2018-09-01 10:07:27,Hobbies > Model Trains & Railway Sets > Rail Vehicles > Trains,226.246.83.170\n"))
                .withCSVParser(new CSVParserBuilder().withQuoteChar(NULL_CHARACTER).build()).build()) {
            String[] record = null;
            while ((record = reader.readNext()) != null) {
                System.out.println(record[0]);
            }
        }
    }
}
