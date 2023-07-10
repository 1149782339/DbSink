package io.dbsink.connector.sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Banner
 *
 * @author Wang Wei
 * @time: 2023-07-10
 */
public class Banner {
    private final static Logger LOGGER = LoggerFactory.getLogger(Banner.class);
    private static List<String> banner = new ArrayList<>();

    static {
        loadBanner();
    }

    private static void loadBanner() {
        InputStream inputStream = Banner.class.getResourceAsStream("/banner");
        try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
            String line = "";
            while ((line = br.readLine()) != null) {
                banner.add(line);
            }
        } catch (IOException e) {
            //ignore
        }
    }

    /**
     * Print banner
     *
     * @author Wang Wei
     * @time: 2023-07-10
     */
    public static void print() {
        for (String line : banner) {
            LOGGER.info("{}              ", line);
        }
    }
}
