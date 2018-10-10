package com.griddynamics.training.vk;

import com.opencsv.CSVWriter;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class RandomEventsProducer {

    private static final long SECONDS_PER_HALF_DAY = TimeUnit.DAYS.toSeconds(1) / 2;
    private static final DateTimeFormatter PURCHASE_DATETIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final Random IP_RANDOM = new Random(1024);
    private static final Random DATETIME_RANDOM = new Random(1096);

    private static String getRandomIP() {
        return IP_RANDOM.nextInt(256) + "." + IP_RANDOM.nextInt(256) + "." + IP_RANDOM.nextInt(256) + "." + IP_RANDOM.nextInt(256);
    }

    private static String getRandomDateAndTime(LocalDate fromDate, int rangeInDays) {
        LocalDate newDate = fromDate.plusDays(DATETIME_RANDOM.nextInt(rangeInDays));
        long secondOfDay;
        do {
            secondOfDay = (long) (SECONDS_PER_HALF_DAY + DATETIME_RANDOM.nextGaussian() * SECONDS_PER_HALF_DAY);
        } while (secondOfDay < 0 || secondOfDay > 86399);
        LocalTime newTime = LocalTime.ofSecondOfDay(secondOfDay);
        return LocalDateTime.of(newDate, newTime).format(PURCHASE_DATETIME_FORMAT);
    }

    private static String convertPrice(String price) {
        if (price == null || price.isEmpty()) return "";
        int indexOfSpace = price.indexOf(' ');
        if (indexOfSpace >= 0) {
            price = price.substring(0, indexOfSpace);
        }
        if (price.length() > 0) {
            price = price.substring(1);
        }
        return price.replaceAll(",", "");
    }

    public static void main(String[] args) throws Exception {
        InputStreamReader amazonEcommerceSampleReader = new InputStreamReader(
                RandomEventsProducer.class.getResourceAsStream("/amazon_co-ecommerce_sample.csv"));

        LocalDate purchaseDateRangeStart = LocalDate.of(2018, Month.SEPTEMBER, 1);

//        List<String> productsFromSpark;
//        try (BufferedReader bufferedReader = new BufferedReader(new FileReader("c:\\Users\\Vladimir\\Downloads\\part-00000-effe25fe-c211-4ea2-b694-64e2d067408f-c000.csv"))) {
//            productsFromSpark = bufferedReader.lines().map(s -> {
//                if (s.startsWith("\"")) {
//                    return s.substring(1, s.length() - 1);
//                } else {
//                    return s;
//                }
//            }).collect(Collectors.toList());
//        }

        try (Socket clientSocket = new Socket("localhost", 61001);
//             CSVWriter csvWriter = new CSVWriter(new FileWriter("c:/temp/random-events-producer-4.csv"), ',', '"', '\\', "\n");
             CSVWriter csvWriter = new CSVWriter(new PrintWriter(clientSocket.getOutputStream(), true), ',', '"', '\\', "\n");
//             CSVPrinter csvPrinter = new CSVPrinter(new PrintWriter(clientSocket.getOutputStream()), CSVFormat.DEFAULT
//                     .withEscape('\\').withQuoteMode(QuoteMode.NONE));
             CSVParser csvReader = new CSVParser(amazonEcommerceSampleReader, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
            int counter = 0;
            for (CSVRecord csvRecord : csvReader) {
                String productName = csvRecord.get(1);
                String categoryName = csvRecord.get(8);
                String productPrice = csvRecord.get(3);
                String purchaseDate = getRandomDateAndTime(purchaseDateRangeStart, 7);
                String clientIP = getRandomIP();
                String[] nextLine = {productName, convertPrice(productPrice), purchaseDate, categoryName, clientIP};
                csvWriter.writeNext(nextLine, false);
                csvWriter.flush();
                counter++;
                System.out.println(counter + "\t" + String.join(",", nextLine));
//                if (categoryName.isEmpty() && !productsFromSpark.contains(productName)) {
//                    System.out.println(productName + "\t[" + categoryName + "]");
//                    counter++;
//                }
            }
            csvWriter.flush();
            System.out.println("counter = " + counter);
        }
    }
}
