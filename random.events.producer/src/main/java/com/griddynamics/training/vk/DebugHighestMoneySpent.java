package com.griddynamics.training.vk;

import com.griddynamics.training.vk.hive.udf.IpToGeoIdUDF;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import java.io.FileReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DebugHighestMoneySpent {

    public static void main(String[] args) throws Exception {
        IpToGeoIdUDF ipToGeoIdUDF = new IpToGeoIdUDF();

        try (CSVReader reader = new CSVReaderBuilder(new FileReader("c:\\temp\\random-events-producer-4.csv"))
                .withCSVParser(new CSVParserBuilder().withEscapeChar('\\').withQuoteChar('"').withSeparator(',').build()).build()) {
            String[] record;
            Map<Integer, Double> moneySpent = new HashMap<>();
            Map<Integer, Integer> transactionsByCountry = new HashMap<>();
            Map<Integer, List<String>> invalidPrices = new HashMap<>();
            while ((record = reader.readNext()) != null) {
                String ip = record[4];
                int geoId = ipToGeoIdUDF.evaluate(ip);
                double price = 0;
                try {
                    price = record[1].length() > 0 ? Double.parseDouble(record[1]) : 0;
                } catch (NumberFormatException e) {
                    invalidPrices.computeIfAbsent(geoId, (key) -> new ArrayList<>()).add(record[1]);
                }

                moneySpent.merge(geoId, price, (a, b) -> b + a);
                transactionsByCountry.merge(geoId, 1, (a, b) -> b + a);
            }

            DecimalFormat df2 = new DecimalFormat(".##");
            moneySpent.entrySet().stream().sorted(Map.Entry.<Integer, Double>comparingByValue().reversed())
                    .forEach(e -> System.out.println(e.getKey() + "\t" + transactionsByCountry.get(e.getKey()) + "\t" + df2.format(e.getValue())));

            System.out.println();

            invalidPrices.entrySet().forEach(System.out::println);
        }

    }
}
