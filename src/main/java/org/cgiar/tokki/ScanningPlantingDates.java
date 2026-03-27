package org.cgiar.tokki;

// Java utilities
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.concurrent.Callable;

// ScanningPlantingDates class
public class ScanningPlantingDates implements Callable<Object[]> 
{
    int medianPlantingDate;
    String weatherFileName;
    String cropCode;

    ScanningPlantingDates(int medianPlantingDate, String weatherFileName, String cropCode)
    {
        this.medianPlantingDate = medianPlantingDate;
        this.weatherFileName = weatherFileName;
        this.cropCode = cropCode;
    }

    @Override
    public Object[] call()
    {

        // To return
        int selectedDate;

        // Container
        TreeMap<Integer, Integer> rainfallByDate = new TreeMap<>();
        for (int i=1; i<=366; i++)
            rainfallByDate.put(i, 0);

        // Read in all rainfall values
        try
        {
            File wtgFile = new File(App.directoryWeather+weatherFileName);
            try (Scanner wtg = new Scanner(wtgFile))
            {
                while (wtg.hasNextLine())
                {
                    String line = wtg.nextLine();
                    String[] values = line.split("\\s+");
                    if (values.length==5 && Utility.isNumeric(values[0].substring(2)))
                    {
                        int ddd  = Integer.parseInt(values[0].substring(2));
                        int newRain = (int)Double.parseDouble(values[4]);
                        int previousRain = rainfallByDate.get(ddd);
                        rainfallByDate.put(ddd, previousRain+newRain);
                    }
                }
            }
        }
        catch (FileNotFoundException | NumberFormatException | StringIndexOutOfBoundsException ex)
        {
            System.err.println("> Weather file scanning error for " + weatherFileName + " (" + ex + "). Returning the median planting date...");
        }

        // Summing for 5-day window
        TreeMap<Integer, Integer> sumOfRainFor5Days = new TreeMap<>();
        for (int i=0; i<rainfallByDate.size(); i++)
        {
            int r = 0;
            for (int j=1; j<=5; j++)
            {
                int d = i + j;
                if (d>365) d = d - 365;
                r += rainfallByDate.get(d);
            }
            sumOfRainFor5Days.put(i, r);
        }

        // Selecting a good date around the median planting date
        ArrayList<Integer> datesToCheck = new ArrayList<>();
        int windowStart = medianPlantingDate - 30;
        int windowEnd = medianPlantingDate + 30;
        for (int i=windowStart; i<=windowEnd; i++)
        {
            if (i < 1)
            {
                int d = 365 + i;
                datesToCheck.add(d);
            }
            else if (i > 365)
            {
                int d = i - 365;
                datesToCheck.add(d);
            }
            else
            {
                datesToCheck.add(i);
            }
        }

        // Scanning
        selectedDate = datesToCheck.get(0);
        int rain5Days = sumOfRainFor5Days.get(selectedDate);
        for (int i=1; i<datesToCheck.size(); i++)
        {
            int nextDate = datesToCheck.get(i);
            int nextRain5Days = sumOfRainFor5Days.get(nextDate);
            if (nextRain5Days>rain5Days)
                selectedDate = nextDate;
        }

        // Return
        String key = weatherFileName.split("\\.")[0] + "_" + cropCode;
        if (App.verbose) System.out.println("> Planting date for this location: "+key+" on "+selectedDate);
        return new Object[] { key, selectedDate };

    }    

}
