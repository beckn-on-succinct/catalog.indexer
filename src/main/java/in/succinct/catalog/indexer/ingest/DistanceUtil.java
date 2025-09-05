package in.succinct.catalog.indexer.ingest;

import com.venky.core.string.StringUtil;

import java.util.HashMap;
import java.util.Map;

public class DistanceUtil {
    private static final Map<String, Double> conversionFactor = new HashMap<>() {{
        put("km", 1.0);
        put("mile", 1.609344);
    }};

    public static double convertDistanceToKm(double distance, String fromUnit) {
        Double f = conversionFactor.get(StringUtil.singularize(fromUnit == null ? "km" : fromUnit).toLowerCase());
        if (f == null){
            throw new RuntimeException("Please specifify distance in miles or kms");
        }
        return f * distance;
    }

}
