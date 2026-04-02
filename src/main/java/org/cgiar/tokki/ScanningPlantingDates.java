package org.cgiar.tokki;

// Java utilities
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.concurrent.Callable;

// ScanningPlantingDates class
// For each simulation year, detects the agronomic onset of the rainy season within a
// ±windowHalfDays window around the median planting date, using:
//   1. A minimum TMIN gate (crop-specific)
//   2. An onset trigger  : cumulative rainfall over the next onsetDays days >= onsetRainfallMm
//   3. A false-start check: no dry spell longer than drySpellMaxDays in the following 30 days
// Falls back to the maximum 5-day rainfall within the window when no onset passes all tests.
public class ScanningPlantingDates implements Callable<Object[]>
{
    int medianPlantingDate;
    String weatherFileName;
    String cropCode;
    int firstYear;
    int numberOfYears;
    final ConsoleProgress plantingScanProgress;

    ScanningPlantingDates(int medianPlantingDate, String weatherFileName, String cropCode,
                          int firstYear, int numberOfYears, ConsoleProgress plantingScanProgress)
    {
        this.medianPlantingDate = medianPlantingDate;
        this.weatherFileName    = weatherFileName;
        this.cropCode           = cropCode;
        this.firstYear          = firstYear;
        this.numberOfYears      = numberOfYears;
        this.plantingScanProgress = plantingScanProgress;
    }

    @Override
    public Object[] call()
    {
        // Crop-specific onset criteria
        int[] criteria       = Utility.getPlantingDateCriteria(cropCode);
        int tminThreshold    = criteria[0];   // °C
        int onsetRainfallMm  = criteria[1];   // mm threshold over onsetDays
        int onsetDays        = criteria[2];   // look-ahead window for onset trigger (days)
        int drySpellMaxDays  = criteria[3];   // max consecutive dry days allowed in false-start check
        int windowHalfDays   = criteria[4];   // ± days around median planting date

        // Parse entire weather file into TreeMap<YYYYDDD, [rain, tmin]>
        // Key is the raw integer from column 1 (e.g. 2021135 for 2021 day 135).
        // TreeMap ordering is correct because YYYYDDD integers are monotonically
        // increasing across year boundaries (2021365 < 2022001).
        TreeMap<Integer, double[]> weatherByDay = new TreeMap<>();
        try
        {
            File wtgFile = new File(App.directoryWeather + weatherFileName);
            try (Scanner wtg = new Scanner(wtgFile))
            {
                while (wtg.hasNextLine())
                {
                    String line   = wtg.nextLine().trim();
                    String[] vals = line.split("\\s+");
                    // Data rows: 5 columns, first column is exactly 7 numeric characters (YYYYDDD)
                    if (vals.length == 5 && vals[0].length() == 7 && Utility.isNumeric(vals[0]))
                    {
                        try
                        {
                            int    yyyyddd = Integer.parseInt(vals[0]);
                            double rain    = Double.parseDouble(vals[4]);  // RAIN column
                            double tmin    = Double.parseDouble(vals[3]);  // TMIN column
                            weatherByDay.put(yyyyddd, new double[]{ rain, tmin });
                        }
                        catch (NumberFormatException ignored) { }
                    }
                }
            }
        }
        catch (FileNotFoundException | StringIndexOutOfBoundsException ex)
        {
            System.err.println("> Weather file scanning error for " + weatherFileName
                    + " (" + ex + "). Using median planting date for all years.");
        }

        // Flat sorted list of day keys — used for index-based look-ahead
        List<Integer> allDays = new ArrayList<>(weatherByDay.keySet());

        // For each simulation year, determine the optimal planting date
        String keyPrefix = weatherFileName.split("\\.")[0] + "_" + cropCode;
        TreeMap<Integer, Integer> yearToPlantingDate = new TreeMap<>();

        for (int yr = firstYear; yr < firstYear + numberOfYears; yr++)
        {
            int ddd = findOnsetDate(yr, medianPlantingDate, windowHalfDays,
                    tminThreshold, onsetRainfallMm, onsetDays, drySpellMaxDays,
                    weatherByDay, allDays);
            yearToPlantingDate.put(yr, ddd);
            if (App.verbose)
                System.out.println("> " + keyPrefix + "_" + yr + " → planting DDD " + ddd);
        }

        if (plantingScanProgress != null)
            plantingScanProgress.step();

        return new Object[] { keyPrefix, yearToPlantingDate };
    }

    // -------------------------------------------------------------------------
    // Core algorithm
    // -------------------------------------------------------------------------

    private int findOnsetDate(int year, int medianDDD, int windowHalfDays,
                              int tminThreshold, int onsetRainfallMm, int onsetDays,
                              int drySpellMaxDays,
                              TreeMap<Integer, double[]> weatherByDay, List<Integer> allDays)
    {
        if (allDays.isEmpty()) return medianDDD;

        // Compute window bounds as indices into allDays
        int centerIdx = indexOfClosestKey(allDays, year * 1000 + medianDDD);
        if (centerIdx < 0) return medianDDD;

        int startIdx = Math.max(0,                centerIdx - windowHalfDays);
        int endIdx   = Math.min(allDays.size()-1, centerIdx + windowHalfDays);

        // Scan candidates earliest → latest within the window
        for (int idx = startIdx; idx <= endIdx; idx++)
        {
            int candidateKey = allDays.get(idx);
            double[] day     = weatherByDay.get(candidateKey);

            // 1. Temperature gate
            if (day[1] < tminThreshold) continue;

            // 2. Onset trigger: cumulative rainfall over next onsetDays days
            double onsetSum = sumRainfall(idx, onsetDays, allDays, weatherByDay);
            if (onsetSum < onsetRainfallMm) continue;

            // 3. False-start check: no dry spell > drySpellMaxDays in following 30 days
            if (hasFalseStart(idx + 1, 30, drySpellMaxDays, allDays, weatherByDay)) continue;

            // Valid onset found — return the DDD part of YYYYDDD
            return candidateKey % 1000;
        }

        // Fallback: pick the day with the highest 5-day cumulative rainfall
        return fallbackMaxRainfall(startIdx, endIdx, allDays, weatherByDay);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Index of the entry whose key is closest to and >= targetKey, or -1 if none. */
    private int indexOfClosestKey(List<Integer> allDays, int targetKey)
    {
        for (int i = 0; i < allDays.size(); i++)
            if (allDays.get(i) >= targetKey) return i;
        return -1;
    }

    /** Cumulative rainfall over `days` consecutive entries starting at index `startIdx`. */
    private double sumRainfall(int startIdx, int days,
                               List<Integer> allDays,
                               TreeMap<Integer, double[]> weatherByDay)
    {
        double sum = 0;
        int end = Math.min(startIdx + days, allDays.size());
        for (int i = startIdx; i < end; i++)
        {
            double[] d = weatherByDay.get(allDays.get(i));
            if (d != null) sum += d[0];
        }
        return sum;
    }

    /**
     * Returns true if there is a dry spell longer than maxDrySpell consecutive days
     * within the next lookAheadDays entries starting at index startIdx.
     * A day is considered dry when daily rainfall < 1 mm.
     */
    private boolean hasFalseStart(int startIdx, int lookAheadDays, int maxDrySpell,
                                  List<Integer> allDays,
                                  TreeMap<Integer, double[]> weatherByDay)
    {
        int consecutiveDry = 0;
        int end = Math.min(startIdx + lookAheadDays, allDays.size());
        for (int i = startIdx; i < end; i++)
        {
            double[] d = weatherByDay.get(allDays.get(i));
            if (d != null && d[0] < 1.0)
            {
                consecutiveDry++;
                if (consecutiveDry > maxDrySpell) return true;
            }
            else
            {
                consecutiveDry = 0;
            }
        }
        return false;
    }

    /** Fallback: return the DDD with the highest 5-day cumulative rainfall in the window. */
    private int fallbackMaxRainfall(int startIdx, int endIdx,
                                    List<Integer> allDays,
                                    TreeMap<Integer, double[]> weatherByDay)
    {
        int    bestKey  = allDays.get(startIdx);
        double bestRain = -1;
        for (int i = startIdx; i <= endIdx; i++)
        {
            double rain5 = sumRainfall(i, 5, allDays, weatherByDay);
            if (rain5 > bestRain)
            {
                bestRain = rain5;
                bestKey  = allDays.get(i);
            }
        }
        return bestKey % 1000;
    }
}
